#!perl
#use Test::More tests => 7;
use Test::More;
use Test::Deep;

use lib (q{E:\work\kmg\gather});
use lib (q{J:\projects\perl\kmg\gather});
use query_graph_object;
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case);
use Carp;
use File::Copy;
$SIG{__DIE__} = sub { Carp::confess( @_ ) };

our $cfg = {};
GetOptions( $cfg, qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s defragment|D stop_on_failure|S tests|t=s init|i rw! debug! ) );

if (!$cfg->{storage} ) {
  $cfg->{storage}='J:\projects\perl\kmg\tests\data\test8db';
  #unlink $cfg->{storage};
}

### init
if ($cfg->{init}) {
  my $initial_core = 'J:\projects\perl\kmg\tests\data\test8db\test4db.json';
  my $core =$cfg->{storage}."/core_graph.json";
  my $junk =$cfg->{storage}."/junk_graph.json";
  my $meta=$cfg->{storage}."/metadata_graph.json";
  unlink $core;
  unlink $junk;
  unlink $meta;
  File::Copy::copy($initial_core, $core);
}

$cfg->{select}//=[qw(none core=rw junk=rw)];

$cfg->{logpath}//='J:\projects\perl\kmg\tests\data\test4';
$cfg->{defragment}//=1;
$cfg->{debug}//=1;
my $g = query_graph_object->new($cfg);
if (0) {
  $g ->get_new_requests();
  ok ( $g->consistency_check(), "data is consistent after get_new_requests");
}
else {
  ok ( $g->consistency_check(), "data is consistent after init");
}

my $req_base = {
    request   => "update_node_info",
    rating    => "45",
    comment   => "comment",
    start     => "20160105",
    end       => "20160112",
    key   => "topic:graph visualisation",
    node_type => "topic",
    #peer_key       => "url:http://stackoverflow.com/questions/7034/graph-visualization-library-in-javascript",
    peer_key       => "url:https://infranodus.com/psychotherapy/plane",
    peer_type => "url",
    edge_type => 'test',
    direction => "out",

  };


my $ops = [
           #{ operation => "delete" },
            { req => { operation => "none"},  result=>{}, ok=>"notning: data is intact", links=>0, nodes=>0} ,
            { req => { operation => "merge"},  match=>1, ok=>"merge: data inserted", links=>1, nodes=>1} ,
            { req => { operation => "delete"}, result=>{}, ok=>"delete: data deleted", links=>-1, nodes=>-1 },
            { req => { operation => "delete"}, ex=>qr/node not found/, ok => "delete: exception: not found", links=>0, nodes=>0 } ,
            { req => { operation => "update"}, ex=>qr/does not exist/, ok=>"update: exception: not found", links=>0, nodes=>0 } ,
            { req => { operation => "insert"}, match=>1  ,  ok=>"insert: data inserted" , links=>1, nodes=>1 },
            { req => { operation => "insert"}, ex=>qr/\balready exists\b/ , ok=>"insert: exception: data already exists" , links=>0, nodes=>0},
            { req => { operation => "update"}, match=>1 , ok=>"update: data updated", links=>0, nodes=>0} ,
            { req => { operation => "merge"},  match=>1 , ok=>"merge: data updated" , links=>0, nodes=>0}  ,
            { req => { operation => "delete"}, result=>{}, ok=>"delete: data deleted" , links=>-1, nodes=>-1 },
          ];

for (my $i=0;$i<@$ops && $i<($cfg->{tests}||100);$i++)  {
  my $op = $ops->[$i];
  my $req = { (%$req_base, %{$op->{req}})} ;
  my $t;
  my $links_before = $g->edge_count();
  my $nodes_before = $g->node_count();
  eval {
      $t = $g->update_node_info($req);
      $g->consistency_check();
      if ($cfg->{rw}) {
        $g->write_db();
        $g = query_graph_object->new($cfg);
        $g->consistency_check();
      }
  };
  my $ex = $@;
  my $links_after= $g->edge_count();
  my $nodes_after = $g->node_count();
  if ($op->{ex}) {
    ok($@=~/$op->{ex}/, sprintf "[%s: %s]: exception: %s", $i, $op->{ok}, substr($@,0,100).'...');
  }
  elsif ($@) {
    fail(sprintf "[%s: %s], exception: %s", $i, $op->{ok}, $@);
  }
  elsif ($op->{match}) {
    if (!is_deeply($t, $req, sprintf "[%s: %s]", $i, $op->{ok})
        && $i==0 ) {
      BAIL_OUT("aborting\n");
    }
  }
  elsif ($op->{result}) {
    is_deeply($t, $op->{result}, sprintf "[%s: %s]", $i, $op->{ok});
  }
  else {
    fail("unknown outcome");
  }
  is ($links_after, $links_before+$op->{links}, "link count correct");
  is ($nodes_after, $nodes_before+$op->{nodes}, "node count correct");
  if ($ex&& $cfg->{stop_on_failure} ) {
    exit;
  }
}

=pod
{
  my $r;
  $g->defragment();
  eval { $r=$g->consistency_check(); };
  if ($@) {
    fail("consistency_check after defrag: got exception: $@");
  }
  ok($@=~/$op->{result}/, $op->{ok});
};


{
  my $req = $req_base;
  $req->{operation}='merge';
  my $t = $g->update_node_info($req);
  my $node= $g->get_node('topic:graph visualisation');
  ok($node, "found inserted node");
  delete $g->{nodes}[$node];
  my $r;
  eval { $r=$g->consistency_check(); };
  ok ($@=~/\bbroken or duplicated.*from=1, to=-/, sprintf "orphaned link exception: %s", $@, 0, 100);
};
=cut
