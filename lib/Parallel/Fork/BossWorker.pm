package Parallel::Fork::BossWorker;

use 5.008008;
use strict;
use warnings;

# Perl module variables
our @ISA = qw();
our $VERSION = '0.01';

sub new {
	my $class = shift;
	my %values = @_;
	
	my $self = {
		result_handler => $values{result_handler} || undef,  # Method for handling output of the workers
		worker_count   => $values{worker_count}   || 10,     # Number of workers
		global_timeout => $values{global_timeout} || 0,      # Number of seconds before the worker terminates the job, 0 for unlimited
		work_handler   => $values{work_handler},             # Handler which will process the data from the boss
		work_queue     => []
	};
	bless $self, ref($class) || $class;

	# The work handler is required
	if (not defined $self->{work_handler}) {
		croak("Parameters \`work_handler' is required.");
	}

	return $self;
}

sub add_work(\@) {
	my $self = shift;
	my $work = shift;
	push(@{ $self->{work_queue} }, pack('u', Storable::freeze($work)));
}

sub process {
	my $self = shift;
	my $handler = shift;
	
	eval {
		
		# If a worker dies, there's a problem
		local $SIG{CHLD} = sub {
			my $pid = wait();
			if (defined $self->{workers}->{$pid}) {
				confess("Worker $pid died.");
			}
		};
		
		# Start the workers
		$self->start();
		
		# Handy pipe reference
		my $from_workers = $self->{from_workers};
		
		# Read from the workers, loop until they all shut down
		while(my $result = &receive($from_workers)) {
			
			# Break the result up into it's pieces
			if ($result =~ m/^(\d+)\n?(.*)?/s) {
			
				# Get the worker's pid and result data
				my $pid = $1;
				my $data = $2;
				
				# Process the result handler
				if (defined $data && defined $self->{result_handler}) {
					&{ $self->{result_handler} }(
						Storable::thaw(
							unpack('u', $data)
						)
					);
				}
				
				# If there's still work to be done, send it to the worker, otherwise shut it down
				if ($#{ $self->{work_queue} } > -1) {
					my $worker = $self->{workers}->{$pid};
					&send($worker, pop(@{ $self->{work_queue} }));
				} else {
					my $fh = $self->{workers}->{$pid};
					delete($self->{workers}->{$pid});
					close($fh);
				}
			} else {
				confess("Malformed message received from worker:\n----SNIP----\n$result\n----SNIP----\n");
			}
		}
	};
	
	if ($@) {
		croak($@);
	}
}

sub start {
	my $self = shift();
	
	# Create a pipe for the workers to communicate to the boss
	pipe($self->{from_workers}, $self->{to_boss});
	
	# Create the workers
	foreach (1..$self->{worker_count}) {
		
		# Open a pipe for the worker
		pipe(my $from_boss, my $to_worker);
		
		# Fork off a worker
		my $pid = fork();
		
		if ($pid > 0) {
			
			# Boss
			$self->{workers}->{$pid} = $to_worker;
			
		} elsif ($pid == 0) {
			
			# Worker
			
			# Close unused pipes
			close($self->{from_workers});
			close($to_worker);
			open(STDIN, '/dev/null');
			
			# Setup communication pipes
			my $to_boss = $self->{to_boss};
			
			# Send the initial request
			&send($to_boss,"$$");
			
			# Start processing
			&worker($self->{to_boss}, $from_boss, $self->{work_handler}, $self->{global_timeout});
			
			# When the worker subroutine completes, exit
			exit;
		} else {
			confess("Failed to fork: $!");
		}
	}
	
	# Close unused pipes
	close($self->{to_boss});
	delete($self->{to_boss});
}

sub worker(\*\*\&\&\&$) {
	my ($to_boss, $from_boss, $work_handler, $timeout) = @_;
	
	# Read instructions from the server
	while (my $input = &receive($from_boss)) {
		
		# If the handler's children die, that's not our business
		$SIG{CHLD} = 'IGNORE';
		
		# Unserialize the boss' instructions
		my $instructions = Storable::thaw(unpack('u', $input));
		
		# Execute the handler with the given instructions
		my $result;
		eval {
			# Handle alarms
			local $SIG{ALRM} = sub {
				die "Work handler timed out."
			};
			
			# Set alarm
			alarm($timeout);
			
			# Execute the handler and get it's result
			if (defined $work_handler) {
				$result = &{ $work_handler }($instructions);
			}
			
			# Disable alarm
			alarm(0);
		};
		
		# Warn on errors
		if ($@) {
			croak("Worker $$ error: $@");
		}
		
		# Send the result to the server
		if (defined $result) {
			&send($to_boss, "$$\n" . pack('u', Storable::freeze($result)));
		} else {
			&send($to_boss, "$$");
		}
	}
}

sub receive(\*) {
	my $fh = shift;
	
	# Save the current file handle
	my $old_fh = select();
	
	# Set the "current" file handle to the new value
	select($fh);
	
	# Set the input record separator
	local $/ = "\t";
	
	# Get a value from the file handle
	my $value = <$fh>;
	
	# Restore the "current" file handle
	select($old_fh);
	
	# Return the value from the file handle
	return $value;
}

sub send(\*$) {
	my ($fh, $value) = @_;
	
	# Save the current file handle
	my $old_fh = select();
	
	# Set the "current" file handle to the new value
	select($fh);
	
	# Set the output record separator
	local $\ = "\t";
	
	# Print the value to the file handle
	print $value;
	
	# Force the file handle to flush
	$| = 1;
	
	# Restore the "current" file handle
	select($old_fh);
}

1;
__END__

=head1 NAME

Parallel::Fork::BossWorker - Perl extension for easiliy creating forking queue processing applications.

=head1 SYNOPSIS

The minimal usage of Parallel::Fork::BossWorker requires you supply
the work_handler argument which returns a hash reference.

	use Parallel::Fork::BossWorker;
	
	# Create new BossWorker instance
	my $bw = new Parallel::Fork::BossWorker(
		work_handler => sub {
				my $work = shift;
				... do work here ...
				return {};
			}
	);
	
	$bw->add_work({key=>"value"});
	$bw->process();

Additionally, you could specify the result_handler argument, which
is passed the hash reference returned from your work_handler.

	use Parallel::Fork::BossWorker;
	
	# Create new BossWorker instance
	my $bw = new Parallel::Fork::BossWorker(
		work_handler => sub {
				my $work = shift;
				... do work here ...
				return {result => "Looks good"};
			},
		result_handler => sub {
			my $result = shift;
			print "$result->{result}\n";
		}
	);

=head1 DESCRIPTION

Parallel::Fork::BossWorker makes creating multiprocess applications easy.

The module is designed to work in a queue style of setup; with the worker
processes requesting 'work' from the boss process. The boss process
transparently serializes and sends the work data to your work handler, to be
consumed and worked. The worker process then transparently serializes and sends
optional data back to the boss process to be handled in your result handler.

This process repeats until the work queue is empty.

=head1 METHODS

=head2 new(...)

Creates and returns a new Parallel::Fork::BossWorker object.

	my $bw = Parallel::Fork::BossWorker->new(work_handler => \&routine)

Parallel::Fork::BossWorker has options which allow you to customize
how exactly the queue is handled and what is done with the data.

=over 4

=item * C<< work_handler => \&routine >>

The work_handler argument is required, the sub is called with it's first
and only argument being one of the values in the work queue. Each worker calls
this sub each time it receives work from the boss process. The handler may trap
$SIG{ALRM}, which may be called if global_timeout is specified.

The work_handler should clean up after itself, as the workers may call the
work_handler more than once.

=item * C<< result_handler => \&routine >>

The result_handler argument is optional, the sub is called with it's first
and only argument being the return value of work_handler. The boss process
calls this sub each time a worker returns data. This subroutine is not affected
by the value of global_timeout.

=item * C<< global_timeout => $seconds >>

By default, a handler can execute forever. If global_timeout is specified, an
alarm is setup to terminate the work_handler so processing can continue.

=item * C<< worker_count => $count >>

By default, 10 workers are started to process the data queue. Specifying
worker_count can scale the worker count to any number of workers you wish.

Take care though, as too many workers can adversely impact performance, though
the optimal number of workers will depend on what your handlers do.

=head2 add_work(\%work)

Adds work to the instance's queue.

	$bw->add_work({data => "my data"});

=head2 process()

Forks off the child processes and begins processing data.

	$bw->process();

=head1 REQUIREMENTS

This module depends on the following modules:

Carp

Storable

=head1 BUGS

None I'm aware of yet, but I'm sure I'll receive reports :)

=head1 SEE ALSO

=head1 AUTHOR

Jeff Rodriguez, E<lt>jeff@jeffrodriguez.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2007, Jeff Rodriguez

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

   * Redistributions of source code must retain the above copyright notice,
     this list of conditions and the following disclaimer.

   * Redistributions in binary form must reproduce the above copyright notice,
     this list of conditions and the following disclaimer in the documentation
     and/or other materials provided with the distribution.

   * Neither the name of the <ORGANIZATION> nor the names of its contributors
     may be used to endorse or promote products derived from this software
     without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

=cut
