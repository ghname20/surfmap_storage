#!perl
use Test::More tests => 1;
use Test::Deep;
use lib (q{E:\work\kmg\gather});
use lib (q{J:\projects\perl\kmg\gather});
use query_graph_object;
use Data::Dumper;
use Getopt::Long qw(:config);
use Carp;
$SIG{__DIE__} = sub { Carp::confess( @_ ) };

our $cfg={};
GetOptions($cfg, qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s));

my $tempdir = "./db";
$cfg->{storage}//="$tempdir/test0db.json";
$cfg->{logpath}//="./data/test0/logs";

if ($cfg->{storage}=~/\.json$/) {
  unlink $cfg->{storage}
}
else {
  die "wrong storage"
}
my $g = query_graph_object->new($cfg);
eval {
  $g->get_new_requests();
  $g->write_db();
  $g->consistency_check()
};
ok(!$@, "consistency_check: $@");
