package Extra::MysqlSlowLog;

#Extra::MysqlSlowLog {
#   /var/lib/mysql/slow.log : port => 3306, user => monitor, pass => resmon62481
#}

# optional: debug => /path/to/log

use strict;
use warnings;
use base 'Resmon::Module';
use DBI;
use File::Copy;
use File::Basename;
use List::Util qw(sum);

my $logline = 0;
my %metrics;

my %query_types = (
    select => [],
    insert => [],
    update => [],
    delete => []
);

my ($thread_id, $schema, $qchit, $querytime, $locktime, $rowssent, $rowsexamined);
my $query = "";
my $tmpfilename = 'resmon-slowquerylog';

sub mean { return @_ ? sum(@_)/@_ : 0 }

sub identify_query {
  my $query = shift;
  $query =~ s/\R/ /g;
  $query =~ s/\s+/ /g;
  chomp($query);
  return if (length($query) == 0);

  my $unmatched = 1;
  for my $q (split /;\s*/, $query) {
    for my $k (keys %query_types) {
      my $re = qr/^$k/i;
      if ( $q =~ /$re/ ) {
        debug("Matched $k: $q");
        push @{$query_types{$k}}, $querytime*1000;
        $unmatched = 0;
      } 
    }
  }
  if ($unmatched) { debug("Unmatched: <$query>"); }
}

sub debug {
  my $logline = shift;

  our $debug;
  if ($debug) {
    my $filename = $debug;
    open(my $fh, '>>', $filename) or die "Could not open file '$filename' $!";
    print $fh "$logline\n";
    close $fh;
  }

}

sub handler {
  my $self = shift;
  my $config = $self->{'config'};
  my $slowquerylog = $self->{'check_name'};
  my $port = $config->{'port'} || 3306;
  my $user = $config->{'user'};
  my $pass = $config->{'pass'};
  our $debug = $config->{'debug'} || 0;

  my($filename, $dirs, $suffix) = fileparse($slowquerylog);

  move( $slowquerylog, $dirs.$tmpfilename );

  my $dbh = DBI->connect("DBI:mysql::localhost;port=$port", $user, $pass);
  my $select_query = "FLUSH LOGS";
  my $sth = $dbh->prepare($select_query);
  $sth->execute();


  open( my $fh, $dirs.$tmpfilename ) or die "Can't read file";

  while(<$fh>){
    next if !$logline && !/^# User\@Host:/;
    next if /^SET /i;
    next if /^# Time:/i;
    next if /^USE /i;
    next if /^# Full_scan:/i;
    next if /^# Filesort:/i;


    if ($logline && /^# User\@Host:/) {
      identify_query($query);
      $querytime = undef;
      $schema = undef;
      $query = "";
    }

    $logline = 1;
    
    if (/^# Thread_id: (.+)\s*Schema: (.+)\s*QC_hit: (.+)/) {
      ($thread_id, $schema, $qchit) = ($1, $2, $3);
      my $line = "Thread ID: $thread_id, $schema, $qchit";
      debug( $_ );
      debug( $line );
      next;
    }

    if (/^# Query_time: (\S+)\s*Lock_time: (\S+)\s*Rows_sent: (\S+)\s*Rows_examined: (.+)/) {
      ($querytime, $locktime, $rowssent, $rowsexamined) = ($1, $2, $3, $4);
      my $line = "Query Time: $querytime, $locktime, $rowssent, $rowsexamined";
      debug($_);
      debug($line);
      next;
    }

    if (!/^# User\@Host:/) {
      debug("RAW QUERY: <$query>");
      $query = $query . $_;
    }
  }

  identify_query($query); #catch last query in log


  for my $k (sort keys %query_types) {
      debug( uc($k).": ".mean(@{$query_types{$k}})." for ".scalar @{$query_types{$k}}." queries");
      my $qps_avg = $k . '_qps';
      $metrics{"$qps_avg"} = mean(@{$query_types{$k}});
  }

  return \%metrics;

}

1;

