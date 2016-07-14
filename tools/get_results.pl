#!perl
use strict;

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
GetOptions( $cfg, qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s
           defragment|D match_undef|U! filterfile|F=s requestfile|R=s request|r=s filters|f=s testdata|t out|o=s
           max_edges|me=s max_nodes|mn=s shared|D=s
           ) );

$cfg->{storage}//='J:\projects\perl\kmg\SOAP\tests\db\query_graph_db.json';
$cfg->{out}//='./gg_out.json';
$cfg->{defragment}//=1;

my $base =  { name=>'get_graph', testdata=>1,  request => {
    expand=>1 , match_undef=>$cfg->{match_undef}//0,
    max_edges=>$cfg->{max_edges},
    max_nodes=>$cfg->{max_nodes},
      filters=>
        $json->decode(<<'CUT'),
      [{"src_key":"^\\Qurl:http://stackoverflow.com/tags/cytoscape.js/hot\\E$"
      ,"dst_key":"^","start":"2016-01-10","end":"2016-02-06"}
      ,{"dst_key":"^\\Qurl:http://stackoverflow.com/tags/cytoscape.js/hot\\E$"
      ,"src_key":"^","start":"2016-01-10","end":"2016-02-06"}]
CUT
  }
};

my $g = query_graph_object->new($cfg);
#my $node=$g->get_node('url:http://adrianroselli.com/2014/01/comparing-opera-mini-and-chrome.html');
my $parent = $g->get_node(254);
$g->descendants(254, sub {
  my ($node,$nr,$level)=@_;
  my $out = $g->extract_results($node);
  for my $o(@$out) {
    printf "---- %s\n\t%s\n\t%s\n", @$o;
  }
});
