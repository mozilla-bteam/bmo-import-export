#!/usr/bin/perl
use 5.10.1;
use strict;
use warnings;

use File::Basename;
use File::Spec;
use lib $ENV{BUGZILLA_DIR}, "$ENV{BUGZILLA_DIR}/lib", "$ENV{BUGZILLA_DIR}/local/lib/perl5";
BEGIN {
    # load the carton dependencies for this checkout in addition to the bugzilla stuff.
    my $dir = dirname(__FILE__);
    lib->import(File::Spec->catdir($dir, qw(local lib perl5)));
}

use Bugzilla;
use Bugzilla::Group;
use Bugzilla::Constants;
use JSON::XS;

BEGIN { Bugzilla->extensions }

# set Bugzilla usage mode to USAGE_MODE_CMDLINE
Bugzilla->usage_mode(USAGE_MODE_CMDLINE);

my $JSON = JSON::XS->new->canonical(1)->utf8(1);
if ($ENV{PRETTY}) {
    $JSON->pretty(1);
}

my %SEEN_GROUP;

my %FILTER = (
    profiles => sub {
        my $profile = shift;
        return {
            %$profile,
            public_key    => '',
            cryptpassword => '*',
            mfa => undef,
        };
    },
);

binmode STDOUT, ':encoding(utf8)';

my $params = { %{ Bugzilla->params } };
my @system_groups;
foreach my $key (keys %$params) {
    if ($key =~ /group$/ && $params->{$key}) {
        push @system_groups, Bugzilla::Group->check({name => $params->{$key}})->id;
    }
    else {
        delete $params->{$key};
    }
}

export_item({TYPE => 'params', params => $params});
export_table(groups => get_groups(@system_groups));
export_product("Bugzilla");
export_product("bugzilla.mozilla.org");
export_table(keyworddefs => ["is_active"]);
export_table(priority => ["isactive"]);
export_table(op_sys => ["isactive"]);
export_table(setting => ["1"]);
export_table(fielddefs => ["custom and type != 99"]);

my @seen_groups = keys %SEEN_GROUP;
export_table(
    group_group_map => and_selector(
        in_selector( member_id  => @seen_groups ),
        in_selector( grantor_id => @seen_groups )
    )
);


sub export_product {
    my ($name) = @_;
    # for getting flag inclusions from flag types
    export_table(
        products => ["name = ?", $name],
        parents => {
            classifications => sub { [ "id = ?", $_->{classification_id} ] },
            rep_platform => sub { [ "id = ?", $_->{default_platform_id} ] },
            op_sys => sub { [ "id = ?", $_->{default_op_sys_id} ] },
            groups => sub { 
                get_groups( $_->{security_group_id} );
            },
        },
        children => {
            versions => sub { return ["product_id = ?", $_->{id}]  },
            milestones => sub { return ["product_id = ?", $_->{id}]  },
            components => sub {
                my $product_id = $_->{id};
                return (
                    [ "product_id = ? AND isactive", $product_id ],
                    parents => {
                        profiles => sub { 
                            my @user_ids = (
                                $_->{triage_owner_id},
                                $_->{watch_user},
                                $_->{initialqacontact},
                                $_->{initialowner},
                            );
                            get_profiles(grep { $_ } @user_ids),
                        },
                    },
                    children => {
                        component_cc => sub {
                            return (
                                ['component_id = ?', $_->{id}],
                                parents => {
                                    profiles => sub { [ 'userid = ?', $_->{user_id} ] },
                                },
                            )
                        },
                        component_reviewers => sub {
                            return (
                                ['component_id = ?', $_->{id}],
                                parents => {
                                    profiles => sub { [ 'userid = ?', $_->{user_id} ] },
                                },
                            )
                        },
                        flaginclusions => sub {
                            return (
                                [ "product_id = ? AND component_id = ?", $product_id, $_->{id} ],
                                parents => {
                                    flagtypes => sub { get_flagtypes($_->{type_id}) },
                                },
                            );
                        },
                        flagexclusions => sub {
                            return (
                                [ "product_id = ? AND component_id = ?", $product_id, $_->{id} ],
                                parents => {
                                    flagtypes => sub { get_flagtypes($_->{type_id}) },
                                },
                            );
                        },
                    }
                );
            },
            flaginclusions => sub {
                return (
                    [ "product_id = ? AND component_id IS NULL", $_->{id} ],
                    parents => {
                        flagtypes => sub { get_flagtypes($_->{type_id}) },
                    }
                );
            },
            flagexclusions => sub {
                return (
                    [ "product_id = ? AND component_id IS NULL", $_->{id} ],
                    parents => {
                        flagtypes => sub { get_flagtypes($_->{type_id}) },
                    }
                );
            },
            group_control_map => sub {
                return (
                    [ "product_id = ?", $_->{id} ],
                    parents => {
                        groups => sub { get_groups($_->{group_id}) },
                    },
                );
            },
        },
    );
}

sub export_table {
    my ($table, $selector, %param) = @_;
    return unless ref $selector;
    my ($where, @bind) = @$selector;
    my $parents = $param{parents};
    my $children = $param{children};
    my $filter   = $param{filter};
    state $exported = {};
    state $undef    = \1;

    my $sth = Bugzilla->dbh->prepare("SELECT * FROM $table WHERE $where");
    $sth->execute(@bind);
    while (my $row = $sth->fetchrow_hashref) {
        my $digest = Digest::MD5->new;
        foreach my $key (sort keys %$row) {
            my $copy = $row->{$key};
            utf8::encode($copy) if $copy;
            $digest->add($key, $copy // "$undef");
        }
        my $hash = $digest->b64digest;
        if ($exported->{$hash}++) {
            next;
        }

        export_nested_table($parents, $row) if $parents;
        if ($FILTER{$table}) {
            $row = $FILTER{$table}->($row);
        }
        export_item({ TYPE => 'table', $table => $row });
        export_nested_table($children, $row) if $children;
    }
}

sub export_nested_table {
    my ($tables, $row) = @_;

    foreach my $table (keys %$tables) {
        local $_ = $row;
        my @args = $tables->{$table}->($_);
        export_table($table, @args) if @args;
    }
}

sub export_item {
    my $output = $JSON->encode($_[0]);
    utf8::decode($output);
    print $output, "\n";
}

sub get_groups {
    my @groups = grep { $_ } @_;

    return () unless @groups;
    $SEEN_GROUP{$_}++ for @groups;
    return (
        in_selector(id => @groups),
        parents => {
            profiles => sub {
                get_profiles(grep { defined } $_->{owner_user_id});
            },
        }
    );
}

sub get_profiles {
    my (@ids) = grep { $_ } @_;

    return unless @ids;
    return in_selector(userid => @ids);
}

sub get_flagtypes {
    my ($type_id) = @_;

    return (
        [ "id = ?", $type_id ],
        parents => {
            profiles => sub {
                return unless $_->{default_requestee};
                return [ "userid = ?", $_->{default_requestee} ];
            },
            groups => sub {
                get_groups($_->{grant_group_id}, $_->{request_group_id});
            },
            flagtype_comments => sub { ["type_id = ?", $type_id] },
        },
    )
}

sub and_selector {
    my (@sels) = @_;
    return [ 
        join(' AND ', map { shift @$_ } @sels),
        map { @$_ } @sels
    ]
}

sub in_selector {
    my ($field, @vals) = @_;
    return undef unless @vals;
    return [$field . ' IN (' . join(',', ('?') x @vals) . ')', @vals ];
}


