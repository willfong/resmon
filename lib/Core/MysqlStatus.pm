package Core::MysqlStatus;

use strict;
use warnings;

use base 'Resmon::Module';
use DBI;

=pod

=head1 NAME

Core::MysqlStatus - check MySQL global statistics

=head1 SYNOPSIS

Core::MysqlStatus {
   127.0.0.1 : port => 3306, user => foo, pass => bar
}

=head1 DESCRIPTION

This module retrieves the SHOW GLOBAL STATUS of a MySQL service.

=head1 CONFIGURATION

=over

=item check_name

The target of the MySQL service.  This can be represented as an IP 
address or FQDN.

=item port

The port that the target service listens on.  This defaults to 3306.

=item user

The username to connect as.  The user must have SELECT access to the
"mysql" database;

=item pass

The password to connect with.

=back

=head1 METRICS

This check returns a significant number of metrics.  There are too many to go
into detail here.  For more information, refer to the MySQL developer
documentation at http://dev.mysql.com/doc/.

=cut

=head1 NOTES

If you want to retrieve slave metrics, you will need to give the user you are
connecting as REPLICATION CLIENT privileges.

=cut

sub handler {
    my $self = shift;
    my $config = $self->{'config'};
    my $target = $self->{'check_name'};
    my $port = $config->{'port'} || 3306;
    my $user = $config->{'user'};
    my $pass = $config->{'pass'};
    my $dbh = DBI->connect("DBI:mysql::$target;port=$port", $user, $pass);

    my $select_query = "SHOW /*!50002 GLOBAL */ STATUS LIKE 'com_%'";
    my $sth = $dbh->prepare($select_query);
    $sth->execute();

    my %metrics;
    while (my $result = $sth->fetchrow_hashref) {
        $metrics{$result->{'Variable_name'}} = $result->{'Value'};
    }

    my $slave_query = "SHOW SLAVE STATUS";
    my $s_sth = $dbh->prepare($slave_query);
    $s_sth->execute();
    while (my $s_result = $s_sth->fetchrow_hashref) {
      foreach my $field ( keys %{ $s_result } ) {
        $metrics{$field} = $s_result->{$field};
      }
    }

    return \%metrics;
};

1;
