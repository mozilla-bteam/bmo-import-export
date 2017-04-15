#!/usr/bin/perl

use 5.10.1;
use strict;
use warnings;
use lib $ENV{BUGZILLA_DIR}, "$ENV{BUGZILLA_DIR}/lib", "$ENV{BUGZILLA_DIR}/local/lib/perl5";

use File::Spec;
use File::Basename;
BEGIN {
    # load the carton dependencies for this checkout in addition to the bugzilla stuff.
    my $dir = dirname(__FILE__);
    lib->import(File::Spec->catdir($dir, qw(local lib perl5)));
}

use Bugzilla;
use Bugzilla::Constants;

BEGIN { Bugzilla->extensions }

use JSON::XS;
use SQL::Abstract;

# set Bugzilla usage mode to USAGE_MODE_CMDLINE
Bugzilla->usage_mode(USAGE_MODE_CMDLINE);

my $JSON = JSON::XS->new->canonical(1)->utf8(1);

my $sa = SQL::Abstract->new;
my $dbh = Bugzilla->dbh;

$dbh->bz_start_transaction();

$dbh->do(q{DELETE FROM components WHERE id = 1});
$dbh->do(q{DELETE FROM products WHERE id = 1});
$dbh->do(q{DELETE FROM profiles WHERE userid = 2});
$dbh->do(q{DELETE FROM rep_platform});
$dbh->do(q{DELETE FROM op_sys});
$dbh->do(q{DELETE FROM priority});
$dbh->do(q{DELETE FROM setting});
$dbh->do(q{UPDATE profiles SET userid = 2 WHERE login_name != 'nobody@mozilla.org' AND userid = 1});
$dbh->do(q{DELETE FROM groups});

while (my $line = <STDIN>) {
    my $item = $JSON->decode($line);
    my $type = delete $item->{TYPE};
    if ($type eq 'table') {
        my ($table) = keys %$item;
        my $row = $item->{$table};
        my ($sql, @bind) = $sa->insert($table, $row);
	$dbh->do($sql, undef, @bind);
    }
}
$dbh->bz_commit_transaction();
