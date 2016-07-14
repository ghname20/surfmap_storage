#!perl
#use Test::More tests => 7;
######
######   testing query filters

use strict;
use Test::More;
use Test::Deep;

use lib (q{E:\work\kmg\gather});
use lib (q{J:\projects\perl\kmg\gather});
use query_graph_object;
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case);
use Carp;
use JSON::XS;

$SIG{__DIE__} = sub { Carp::confess( @_ ) };
our $json=JSON::XS->new->canonical;
our $cfg = {};
GetOptions( $cfg, qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s defragment|D match_undef|U filter|f=s notestdata|T no-resout|O no-compare|C) );

$cfg->{storage}//='J:\projects\perl\kmg\tests\data\test5\db';
$cfg->{logpath}//='J:\projects\perl\kmg\tests\data\test5\a';
$cfg->{resultpath}//='J:\projects\perl\kmg\tests\data\test5\results';
$cfg->{expectedpath}//='J:\projects\perl\kmg\tests\data\test5\expected';

$cfg->{select}//=[qw(none core=Crw junk=rw)];


if ( !$cfg->{no_init} ) {
  my $initial_core = 'J:\projects\perl\kmg\tests\data\test5\db\test4db.json';
  my $core         = $cfg->{storage} . "/core_graph.json";
  my $junk         = $cfg->{storage} . "/junk_graph.json";
  my $meta         = $cfg->{storage} . "/metadata_graph.json";
  unlink $core;
  unlink $junk;
  unlink $meta;
  File::Copy::copy( $initial_core, $core );
}

$cfg->{defragment}//=1;
my $g = query_graph_object->new($cfg);
my $gnt = query_graph_object->new($cfg);

$g ->get_new_requests();
$gnt ->get_new_requests();
ok ( $g->consistency_check(), "data is consistent after get_new_requests");

my $req_base = {
    request   => "update_node_info",
    direction => "in",
    operation => "merge",
  };

my $edges = [   {
    key   => "topic:test5 graph visualisation",
    node_type => "topic"
  },
  {
    peer_key   => "topic:test5 graph visualisation",
    peer_type  => 'topic',
    #peer_topic => 'test5 graph visualisation',
    node_type => "query",
    key       => "google:test5 graph+visualization+javascript",
    edge_type => 'session',
  },
  {
    peer_key   => "google:test5 graph+visualization+javascript",
        peer_type => "query",
    node_type => "url",
    key       => "url:test5http://stackoverflow.test/questions/7034/graph-visualization-library-in-javascript",
    edge_type => 'jump',

  }, {
    peer_key   => "google:test5 graph+visualization+javascript",
        peer_type => "query",

    node_type => "url",
    key       => "url:test5http://stackoverflow.test/questions/24396708/zoomable-network-graph-in-angularjs",
    edge_type => 'jump',
  },

  {
    key   => "topic:test5 cytoscape.js",
    node_type => "topic",
  },
  {
    peer_key=> "topic:test5 cytoscape.js",
    peer_type => "topic",
    key       => "google:test5 cytoscape.js+tutorial",
    node_type => "query",
    edge_type => 'session',
  },
  {
    peer_key   => "google:test5 cytoscape.js+tutorial",
    peer_type => "query",
    node_type => "url",
    key       => "url:test5http://stackoverflow.test/questions/21650711/live-code-examples-cytoscape-js-initialization-incomplete",
    edge_type => 'jump',
  } ,
  {
    peer_key   => "google:test5 cytoscape.js+tutorial",
    peer_type => "query",
    node_type => "url",
    key       => "url:test5 http://chianti.ucsd.edu.test/~kono/cy3js/",
    edge_type => 'jump',
  }
];

my %stats;
@{$stats{first}}{qw(links nodes)} = ($g->edge_count(), $g->node_count());

unless ($cfg->{notestdata}) {
  for my $e ( @$edges  ){
    my $req = { (%$req_base, %$e)}  ;
    my %tmpstats;
    my $lc = $g->edge_count();
    my $nc = $g->node_count();
    my $nlc = $req ->{peer_key} ? $lc + 1 : $lc;
    my $nnc = $nc + 1;
    #@{$tmpstats{first}}{qw(links nodes)} = ($g->edge_count(), $g->node_count());

    $g->update_node_info($req);
    is ($nlc, $g->edge_count() , "link count correct: $req->{key}" );
    is ($nnc, $g->node_count() , "node count correct: $req->{key}" );

  }
}
ok($g->consistency_check(), "data consistent after updates");
@{$stats{second}}{qw(links nodes)} = ($g->edge_count(), $g->node_count());

is ($stats{first}{links}+@$edges-2, $stats{second}{links}, sprintf "added %d links", @$edges-2);
is ($stats{first}{nodes}+@$edges+0, $stats{second}{nodes}, sprintf "added %d nodes", @$edges+0);

$g->{cfg}{logpath}//='J:\projects\perl\kmg\tests\data\test5\b';
$g ->get_new_requests();
$gnt->{cfg}{logpath}//='J:\projects\perl\kmg\tests\data\test5\b';
$gnt ->get_new_requests();
ok($g->consistency_check(), "data consistent after reading new requests");

my $req0 = { filters=>[
  {
    text=>"cyto",
  }
  ] };
my $req1 = {
  filters =>
  [
  {
    'text' => 'cyt',
    'end' => '2016-01-29',
    'start' => ''
  }
]
};

my $req = { match_undef=>$cfg->{match_undef}//0,
           filters=> [
  {
    #'start' => '2016-01-20',
    start => $cfg->{start}//'2016-01-10',
    'text' => $cfg->{filter}//'windows',
    #'end' => $cfg->{end}//'2016-01-29',
  }
]};

my $tests  = [
  { testdata=>1, request =>
    { match_undef=>$cfg->{match_undef}//0,

             filters=> [
        {
          #'start' => '2016-01-20',
          start => $cfg->{start}//'2016-01-10',
          filter_type=>"edge", "from" => {
          'text' => 'test5',
          }
          #'end' => $cfg->{end}//'2016-01-29',
        }]
    },
  }, { testdata=>0, request =>
    { match_undef=>$cfg->{match_undef}//0,
             filters=> [
        {
          #'start' => '2016-01-20',
          start => $cfg->{start}//'2016-01-10',
          filter_type=>"edge", "from" => {
          'text' => 'test5',
          }
          #'end' => $cfg->{end}//'2016-01-29',
        }]
    },
  }, { testdata=>1, request =>
    { match_undef=>$cfg->{match_undef}//0,
             filters=> [
        {
          #'start' => '2016-01-20',
          start => $cfg->{start}//'2016-01-10',
          filter_type=>"edge", "from" => {
          'text' => 'test5',
          },
          'end' => $cfg->{end}//'2016-01-29',
        }]
    },
  }, { testdata=>1, request =>

    { match_undef=>$cfg->{match_undef}//0,
             filters=> [
        {
          #'start' => '2016-01-20',
          start => $cfg->{start}//'2016-01-10',
          filter_type=>"edge", "from" => {
          'node_type'=>'query',
          'text' => 'test5',
          }
          #'end' => $cfg->{end}//'2016-01-29',
        }]
    },
  }, { testdata=>1, request =>
    { match_undef=>$cfg->{match_undef}//0,
             filters=> [
        {
          #'start' => '2016-01-20',
          start => $cfg->{start}//'2016-01-10',
          filter_type=>"edge", "from" => {
          'node_type'=>'topic',
          'text' => 'test5',
          }
          #'end' => $cfg->{end}//'2016-01-29',
        }]
    },
  }, { testdata=>1, request =>

    { match_undef=>$cfg->{match_undef}//0,
             filters=> [
        {
          #'start' => '2016-01-20',
          start => $cfg->{start}//'2016-01-10',
          filter_type=>"edge", "from" => {
          'node_type'=>'^(?!topic)(?!url)(?!query)',
          'text' => 'test5',
          }
          #'end' => $cfg->{end}//'2016-01-29',
        }]
    },
    },
  { testdata=>1, request =>

    { match_undef=>$cfg->{match_undef}//0,
             filters=> [
        {
          #'start' => '2016-01-20',
          start => $cfg->{start}//'2016-01-10',
#          'type'=>'^(?!topic)(?!url)(?!query)',
          filter_type=>"edge", "from" => {
          'text' => 'test5.*visual',
          }
          #'end' => $cfg->{end}//'2016-01-29',
        }]
    },
    }
];
for (my $i=0;$i<@$tests; $i++) {
  my $test = $tests ->[$i];
  my $req  = $test->{request};
  my $result;
  my ($lc,$nc);
  if ($test->{testdata}) {
    $result = $g->get_graph_data( $req )  ;
    $lc = $g->edge_count();
    $nc = $g->node_count();
  }
  else  {
    $result = $gnt->get_graph_data( $req )  ;
    $lc = $gnt->edge_count();
    $nc = $gnt->node_count();
  }
  my $test_result = {
    testdata=>$test->{testdata},
    test=>$i,
    request=>$req,
    result=>$result,
    node_count=>$nc,
    edge_count=>$lc,
  };
  my $fh;
  if (!$cfg->{'no-resout'}) {
    open($fh, '>:raw', "$cfg->{resultpath}/test_$i.json" ) or die $!;
  }
  else {
    $fh = \*STDOUT;
  }
  print {$fh} $json->pretty->encode($test_result);
  close $fh;
  if (!$cfg->{'no-compare'}) {
    open(my $fh, '<:raw', "$cfg->{resultpath}/test_$i.json" ) or die $!;
    open(my $gh, '<:raw', "$cfg->{expectedpath}/test_$i.json" ) or die $!;
    read ($fh, my $resultjson, -s $fh) or die $!;
    read ($gh, my $expectedjson, -s $gh) or die $!;
    close $fh; close $gh;
    my $result = $json ->decode($resultjson);
    my $expected = $json ->decode($expectedjson);
    is_deeply($result, $expected, "request $i: the result matches expected data");
  }
}
done_testing();
