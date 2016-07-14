#!perl
###########
########### testing save/get/delete
#use Test::More tests => 7;
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

$SIG{__DIE__} = sub { Carp::confess(@_) };
our $json = JSON::XS->new->canonical;
our $cfg  = {};
GetOptions(
  $cfg,
  qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s defragment|D match_undef|U! filter|f=s notestdata|T no-resout|O no-compare|C!
    tests_get|tg=s no_init|I!)
);

$cfg->{storage}      //= 'J:\projects\perl\kmg\tests\data\common';
$cfg->{resultpath}   //= 'J:\projects\perl\kmg\tests\data\test6\results';
$cfg->{expectedpath} //= 'J:\projects\perl\kmg\tests\data\test6\expected';
$cfg->{defragment}   //= 1;

if ( !$cfg->{no_init} ) {
  my $initial_core = 'J:\projects\perl\kmg\tests\data\common\orig20.json';
  my $core         = $cfg->{storage} . "/core_graph.json";
  my $junk         = $cfg->{storage} . "/junk_graph.json";
  my $meta         = $cfg->{storage} . "/metadata_graph.json";
  unlink $core;
  unlink $junk;
  unlink $meta;
  File::Copy::copy( $initial_core, $core );
}

$cfg->{select} //= [qw(none core=rw junk=rw)];

my $g = query_graph_object->new($cfg);

#$g ->get_new_requests();
#ok ( $g->consistency_check(), "data is consistent after get_new_requests");

#
#my $req_json = <<CUT;
#{"request_source":"popup",
#"request":"save_new_elements",
#"method":"save_new_elements"}
#CUT

#my $elem_json =

#my $req  = $json->decode($req_json);
#$req -> {elements} = $json ->decode($elem_json);
#$req -> {dryrun} = 1;

my %stats;

#@{$stats{first}}{qw(links nodes)} = ($g->edge_count(), $g->node_count());

#@{$stats{second}}{qw(links nodes)} = ($g->edge_count(), $g->node_count());

#is ($stats{first}{links}+@$edges-2, $stats{second}{links}, sprintf "added %d links", @$edges-2);
#is ($stats{first}{nodes}+@$edges+0, $stats{second}{nodes}, sprintf "added %d nodes", @$edges+0);

#$g->{cfg}{logpath}//='J:\projects\perl\kmg\tests\data\test5\b';
#$g ->get_new_requests();
#$gnt->{cfg}{logpath}//='J:\projects\perl\kmg\tests\data\test5\b';
#$gnt ->get_new_requests();
#ok($g->consistency_check(), "data consistent after reading new requests");

my $tests_save = [
  {
    testdata    => 1,
    node_change => 1,
    link_change => 1,
    request     => {
      request_source => "popup",
      request        => "save_new_elements",
      method         => "save_new_elements",
      elements       => <<CUT,
[{"comment":null,"id":"n143674","key":"google:cytoscape.js+right+click+canvas","name":"google:cytoscape.js+right+click+canvas","node_type":"query","notable":"","rating":null,"degree":8},
{"id":"newnode_86","name":"newnode_86","node_type":"topic","degree":20,"key":"topic:cytoe"},
{"id":"newedge_63","source":"newnode_86","target":"n143674","edge_type":"related"}]
CUT
    },
  },
  {
    testdata    => 1,
    node_change => 1,
    link_change => 2,
    request     => {
      method         => "save_new_elements",
      request        => "save_new_elements",
      request_source => "popup",
      elements       => <<CUT,
[{"comment":null,"id":"n143670","key":"google:cytoscape.js+add+node+right+click","name":"google:cytoscape.js+add+node+right+click","node_type":"query","notable":"","rating":null,"degree":4},{"comment":null,"id":"n143674","key":"google:cytoscape.js+right+click+canvas","name":"google:cytoscape.js+right+click+canvas","node_type":"query","notable":"","rating":null,"degree":8},{"id":"newnode_86","name":"newnode_86","node_type":"topic","degree":20,"key":"topic:cyto0a"},{"id":"newedge_63","source":"newnode_86","target":"n143670","edge_type":"related"},{"id":"newedge_64","source":"newnode_86","target":"n143674","edge_type":"related"}]
CUT
    },
  },
];

my $tests_save_subset = [ 0 .. $#$tests_save ];

for ( my $i = 0 ; $i < @$tests_save_subset ; $i++ ) {
  my $test = $tests_save->[ $tests_save_subset->[$i] ];
  my $req  = $test->{request};
  $req->{elements} = $json->decode( $req->{elements} );
  my $result;
  my ( $lc, $nc );
  $lc     = $g->edge_count();
  $nc     = $g->node_count();
  $result = $g->save_new_elements($req);
  is( $lc, $g->edge_count() - $test->{link_change}, "link count correct" );
  is( $nc, $g->node_count() - $test->{node_change}, "node count correct" );
}

my $tests_get = [
  {
    testdata => 1,
    request  => {
      match_undef => $cfg->{match_undef} // 0,
      filters => [
        {
          filter_type => "edge",
          "from"      => {
            'text' => 'cytoe|cyto0a',
          }
        }
      ]
    },
  },
  {
    testdata => 1,
    request  => {
      filters => [

        {
          "exact"     => "true",
          filter_type => "edge",
          "from"      => {
            "key" => "url:http://stackoverflow.com/questions/26123468/dynamic-node-content-label-in-cytoscape-js",
          },
          "start" => "2016-01-01",
          "end"   => "2016-02-01"
        }
      ],
      method         => "get_graph_data",
      request        => "get_graph_data",
      request_source => "popup",
    }
  },
  {
    testdata => 1,
    request  => {
      match_undef => $cfg->{match_undef} // 0,

      filters => [
        {
          filter_type => "edge",
          "from"      => { "text" => "cytoscape" },
          "to"        => { "text" => "^" },
          "edge"      => { "edge_type" => "^" }
        }
      ],
    },
  },
  {
    testdata => 1,
    expand   => 0,
    request  => {
      match_undef => $cfg->{match_undef} // 0,
      filters => [ { filter_type => "edge", "from" => { "text" => "cytoscape" } } ],
    },
  },
  {
    testdata => 1,
    request  => {
      expand      => 1,
      match_undef => $cfg->{match_undef} // 0,
      filters     => [
        {
          filter_type => "edge",
          "from"      => { "text" => "novosti"} , "to"=>{ "text" => "^" } ,
        }
      ],
    },
  },
  {
    testdata => 1,
    request  => {
      filters => [
        {
          filter_type => "edge",
          "from"      => {
            "key" => "url:http://stackoverflow.com/questions/26123468/dynamic-node-content-label-in-cytoscape-js",
          },
          "to" => {
            "text" => "^"
          }
        }
      ],
      method         => "get_graph_data",
      request        => "get_graph_data",
      request_source => "popup",
    }
  }
];

my $tests_get_subset = [ 0 .. $#$tests_get ];
if ( $cfg->{tests_get} ne '' ) {
  $tests_get_subset = [ split ',', $cfg->{tests_get} ];
}

for ( my $i = 0 ; $i < @$tests_get_subset ; $i++ ) {
  my $it   = $tests_get_subset->[$i];
  my $test = $tests_get->[ $tests_get_subset->[$i] ];
  my $req  = $test->{request};
  my $result;
  my ( $lc, $nc );
  $result = $g->get_graph_data($req);
  $lc     = $g->edge_count();
  $nc     = $g->node_count();
  my $test_result = {
    testdata   => $test->{testdata},
    test       => $it,
    request    => $req,
    result     => $result,
    node_count => $nc,
    edge_count => $lc,
  };
  my $fh;

  if ( !$cfg->{'no-resout'} ) {
    open( $fh, '>:raw', "$cfg->{resultpath}/test_$it.json" ) or die $!;
  }
  else {
    $fh = \*STDOUT;
  }
  print {$fh} $json->pretty->encode($test_result);
  close $fh;
  if ( !$cfg->{'no-compare'} ) {
    open( my $fh, '<:raw', "$cfg->{resultpath}/test_$it.json" )   or die "$cfg->{resultpath}/test_$it.json: $!";
    open( my $gh, '<:raw', "$cfg->{expectedpath}/test_$it.json" ) or die "$cfg->{expectedpath}/test_$it.json:$!";
    read( $fh, my $resultjson,   -s $fh ) or die $!;
    read( $gh, my $expectedjson, -s $gh ) or die $!;
    close $fh;
    close $gh;
    my ( $result, $expected );
    eval {
      $result   = $json->decode($resultjson);
      $expected = $json->decode($expectedjson);
    };
    if ($@) {
      die "json decoding errors:$@";
    }
    is_deeply( $result, $expected, "request $it: the result matches expected data" );
  }
}

my $tests_delete = [
  {
    testdata    => 1,
    node_change => -1,
    link_change => -1,
    ,
    request => {
      request   => "update_node_info",
      key       => 'topic:cytoe',
      node_type => 'topic',
      operation => 'delete',
    }
  },
  {
    testdata    => 1,
    node_change => -1,
    link_change => -2,
    request     => {
      request   => "update_node_info",
      key       => 'topic:cyto0a',
      node_type => 'topic',
      operation => 'delete',
    }
  },
];
for ( my $i = 0 ; $i < @$tests_save_subset ; $i++ ) {
  my $test = $tests_delete->[ $tests_save_subset->[$i] ];
  my $req  = $test->{request};
  my $result;
  my ( $lc, $nc );
  $lc     = $g->edge_count();
  $nc     = $g->node_count();
  $result = $g->update_node_info($req);
  is( $lc, $g->edge_count() - $test->{link_change}, "link count correct" );
  is( $nc, $g->node_count() - $test->{node_change}, "node count correct" );
  $g->consistency_check();
}

done_testing();
