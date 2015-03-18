package Core::Memstat;

use strict;
use warnings;

use base 'Resmon::Module';

use Resmon::ExtComm qw(run_command cache_command);

my $osname = $^O;
my $usekstat = 0;
my $kstat;
if ($osname eq 'solaris') {
    eval "use Sun::Solaris::Kstat";
    unless ($@) {
        $usekstat = 1;
        $kstat = Sun::Solaris::Kstat->new();
    }
}

=pod

=head1 NAME

Core::Memstat - Monitor memory statistics using vmstat

=head1 SYNOPSIS

 Core::Memstat {
     local : noop
 }

 Core::Memstat {
     local : vmstat_path => /usr/sbin/vmstat
 }

=head1 DESCRIPTION

This module returns statistics on active and free memory.  The type and
number of metrics returned depend on the capabilities of each platform's
respective vmstat command.

=head1 CONFIGURATION

=over

=item check_name

The check name is used for descriptive purposes only.  It is not used
for anything functional.

=item vmstat_path

Provide an alternate path to the vmstat command (optional).

=back

=head1 METRICS

The metrics returned by this module vary by OS and method used:

=head2 VMSTAT METRICS

The default/fallback method used is vmstat, which returns the following
metrics:

=over

=item actv_mem

Active virtual pages.

=item free_mem

Free real memory.

=back

=head2 KSTAT METRICS

Solaris provides an interface to numerous kernel statistics.  If the
Perl Sun::Solaris::Kstat library is locally available, this module will
prefer that method first, bypassing vmstat collection.  Otherwise, this
module falls back on the standard vmstat collection method.

=head2 PROC METRICS

Linux configurations use /proc/meminfo for memory statistics.

=head2 SYSCTL METRICS

FreeBSD configurations use sysctl to extract the most common memory
statistics.  With the exception of hw.physmem, all metrics are pulled
from the vm.stats.vm branch.

=cut

sub handler {
    my $self = shift;
    my $disk = $self->{'check_name'};
    my $config = $self->{'config'};
    my $vmstat_path = $config->{'vmstat_path'} || 'vmstat';

    if ($osname eq 'solaris') {
        my $pagesize = run_command('pagesize');
        if ($usekstat && $pagesize) {
            my %metrics;
            $kstat->update();
            my $syspages = $kstat->{'unix'}->{0}->{'system_pages'};

            foreach (keys %$syspages) {
                $metrics{"kstat_${_}"} = [int($syspages->{$_} * $pagesize / 1024), 'L'] unless ($_ eq 'class');
            }
            if (exists $kstat->{'zfs'}) {
                $metrics{'kstat_cache_mem'} = [int($kstat->{'zfs'}->{0}->{'arcstats'}->{'size'} / 1024), 'L'];
            }
            return \%metrics;
        } else {
            my $output = run_command("$vmstat_path");
            if ($output =~ /.*cs\s+us\s+sy\s+id\n\s+\d+\s+\d+\s+\d+\s+(\d+)\s+(\d+).*/) {
                return {
                    'actv_mem' => [$1, 'L'],
                    'free_mem' => [$2, 'L']
                };
            } else {
                die "Unable to extract statistics\n";
            }
        }
    } elsif ($osname eq 'linux') {
        my %metrics;
        my %ret_metrics;
        open(MEMINFO, '/proc/meminfo') || die "Unable to read proc: $!\n";
        while (<MEMINFO>) {
            /(\w+)\:\s+(\d+).*/;
            next unless defined($1);
            $metrics{$1} = [$2, 'L'];
        }
        close(MEMINFO);
        $metrics{MemFreeBufCache} = [
            ($metrics{MemFree}[0] + $metrics{Buffers}[0] +
                $metrics{Cached}[0]), "L"];
        $ret_metrics{MemUsagePct} = [
            sprintf("%.4f", ($metrics{MemTotal}[0] - $metrics{MemFree}[0] - 
                $metrics{Buffers}[0] - $metrics{Cached}[0]) / $metrics{MemTotal}[0])*100, "L"];
        return \%ret_metrics;
    } elsif ($osname eq 'freebsd') {
        my %metrics;
        open(SYSCTL, 'sysctl hw.physmem vm.stats.vm |') || die "Unable to read sysctl: $!\n";
        while (<SYSCTL>) {
            /(.*)\:\s+(\d+).*/;
            $metrics{$1} = [$2, 'L'];
        }
        foreach my $page (qw/cache inactive active wire free page/) {
            $metrics{"vm.stats.vm.v_${page}_count"}->[0] *= ($metrics{'vm.stats.vm.v_page_size'}->[0] / 1024);
        }
        close(SYSCTL);
        return \%metrics;
    } elsif ($osname eq 'openbsd') {
        my $output = run_command("$vmstat_path");
        if ($output =~ /.*cs\s+us\s+sy\s+id\n\s+\d+\s+\d+\s+\d+\s+(\d+)\s+(\d+).*/) {
            return {
                'actv_mem' => [$1, 'L'],
                'free_mem' => [$2, 'L']
            };
        } else {
            die "Unable to extract statistics\n";
        }
    } else {
        die "Unsupported platform: $osname\n";
    }
};

1;
