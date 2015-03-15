package Core::Cpu;

use strict;
use warnings;

use base 'Resmon::Module';

use Resmon::ExtComm qw(run_command);

=pod

=head1 NAME

Core::Cpu - check CPU usage

=head1 SYNOPSIS

 Core::Cpu {
    local : vmstat_path => /usr/bin/vmstat
 }

=head1 DESCRIPTION

This module retrieves CPU statistics.

=head1 CONFIGURATION

=over

=item check_name

Arbitrary name of the check.

=item vmstat_path

Optional path to the vmstat executable.

=item tail_path

Optional path to the tail executable.

=back

=head1 METRICS

=over

=item user (time)

=item system (time)

=item idle (time)

=back

=cut

sub handler {
    my $self = shift;
    my $config = $self->{'config'};
    my $vmstat_path = $config->{'vmstat_path'} || 'vmstat';
    my $tail_path = $config->{'tail_path'} || 'tail';
    my $output = run_command("$vmstat_path 1 2 | $tail_path -1");
    my $osname = $^O;
    my %metrics;
    my @keys = qw( user system idle );
    my @values;

    $output =~ s/^\s+//;
    $output =~ s/\s+/ /g;
    if ($osname eq 'solaris') {
        @values = (split(/\s+/, $output))[-3..-1];
    } elsif ($osname eq 'linux') {
        if ( scalar split(/\s+/, $output) == 16 ){
            @values = (split(/\s+/, $output))[-4..-2];
        } elsif( scalar split(/\s+/, $output) == 17 ) {
            @values = (split(/\s+/, $output))[-5..-3];
        }

        
    } elsif ($osname eq 'openbsd') {
        @values = (split(/\s+/, $output))[-3..-1];
    } elsif ($osname eq 'freebsd') {
        @values = (split(/\s+/, $output))[-3..-1];
    } else {
        die "Unknown platform: $osname";
    }

    %metrics = map { $_ => shift(@values) } @keys;
    for my $key (keys %metrics) {
        $metrics{$key} = [$metrics{$key}, 'I'];
    }

    return \%metrics;
};

1;
