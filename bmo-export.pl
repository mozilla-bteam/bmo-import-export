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

my $USERS = $JSON->decode(
    do {
        local $/ = undef;
        open my $fh, '<', "profiles.json";
        scalar <$fh>;
    }
);

my %NEW_USER;
my %SEEN_GROUP;

my %FILTER = ();

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

push @system_groups, Bugzilla::Group->check({name => "query_database"});
push @system_groups, Bugzilla::Group->check({name => "admin"});

export_table(profiles => ["userid = ?", 1]);
export_table(groups => get_groups(@system_groups));
export_product("Bugzilla");
export_product("Core");
export_product("bugzilla.mozilla.org");
export_table(keyworddefs => ["is_active"]);
export_table(priority => ["isactive"]);
export_table(op_sys => ["isactive"]);
export_table(setting => ["1"]);
export_item({TYPE => 'params', params => $params});
#export_table(fielddefs => ["custom and type != 99"]);

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
                            new_user($_->{triage_owner_id}, 'triage');
                            new_user($_->{watch_user}, 'watch');
                            new_user($_->{initialqacontact}, 'qa');
                            new_user($_->{initialowner}, 'owner');
                            return;
                        }
                    },
                    children => {
                        component_cc => sub {
                            return (
                                ['component_id = ?', $_->{id}],
                                parents => {
                                    profiles => sub {
                                        new_user($_->{user_id}, 'cc');
                                        return;
                                    },
                                },
                            )
                        },
                        component_reviewers => sub {
                            return (
                                ['component_id = ?', $_->{id}],
                                parents => {
                                    profiles => sub {
                                        new_user($_->{user_id}, 'reviewer');
                                        return;
                                    },
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
                new_user($_->{owner_user_id}, 'group owner');
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
                new_user($_->{default_requestee}, 'flag');
                return;
            },
            groups => sub {
                get_groups($_->{grant_group_id}, $_->{request_group_id});
            },
        },
        children => {
            flagtype_comments => sub { ["type_id = ?", $type_id] },
        },
    )
}

sub new_user {
    my ($id, $role) = @_;
    state $next_id = 1000; # first 999 ids are not used.
    state $cache = {};

    return unless $id;
    return 1 if $id == 1;

    my $user = Bugzilla::User->check({ id => $id });
    if ($cache->{$id}) {
        $_[0] = $cache->{$id};
        $NEW_USER{ $cache->{$id} }{role}{ $role }++;
        return $_[0];
    }
    my $new_id = $next_id++;

    my $row;
    if ($user->login =~ /\.bugs$/) {
        $row = { userid => $new_id, login_name => $user->login };
    }
    else {
        $row = pop @$USERS;
        $row->{userid} = $new_id;
        my ($nick, $other) = split(/\s+/, delete $row->{nick});
        if ($row->{realname} =~ /^(\w)\w+\s+(?:\w\.\s+)?(\w+)$/) {
            state $seen = {};
            my (undef, $host) = split(/@/, $row->{login_name}, 2);
            my $name = "$1$2";
            unless ($seen->{"$name\@$host"}++) {
                $row->{login_name} = "$name\@$host";
            }
            else {
                die "dupe: $name\n";
            }
        }
        else {
            die "real name: $row->{realname}";
        }
        $row->{login_name} = lc $row->{login_name};
        $row->{realname} .= " [:$nick]";
        if (@$USERS % 2) {
            $row->{realname} .= " ($other)";
        }
    }
    export_item({TYPE => 'table', profiles => $row});

    $_[0] = $cache->{$id} = $new_id;
    return $cache->{$id};
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


