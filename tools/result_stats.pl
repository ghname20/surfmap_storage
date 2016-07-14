#!perl
use strict;

use lib (q{E:\work\kmg\gather});
use lib (q{J:\projects\perl\kmg\gather});
use query_graph_object;
use graph_object;
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

$cfg->{storage}//='J:\projects\perl\kmg\scratch\db';
$cfg->{out}//='./stats.json';
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
#my $parent=$g->get_node('url:http://adrianroselli.com/2014/01/comparing-opera-mini-and-chrome.html');
my $node = $g->g->get_node_by_id(2);
$node=  $g->g->get_node_by_key("google:windows+10+latest+build");
$node=  $g->g->get_node_by_key("google:cytoscape.js+best+layout+for+text+nodes");
  #my ($node, $parent, $root, $ffun, $level, @args)=@_;
  #return 1 if descend_dl($root->{descendants}{$ch}, $node, $root, $ffun, $level +1, @args);
  #return 1 if $ffun->($node->{id}, $node, $parent, $root, $level, @args);

$node->descend_dl(undef, $node, sub {
  my ($id,$child,$parent,$root,$level)=@_;
  my $out = $g->get_results($root->id,$child->{id});
  return () if !defined $out;
  #for my $o(@$out) {
  #  printf "---- %s\n\t%s\n\t%s\n", @$o;
  #}
  printf "======= request %s.%s =======\n",  $root->id, $child->{id};
  my $stop = qr{\b(cytoscape|js|layout|text|nodes?)\b}i;
  my $stats=$g->compute_relevance($out, {wordrx=>qr{\w+},stoprx=>$stop});
  #for my $word (sort {$stats->{words}{$b} <=> $stats->{words}{$a} } keys %{$stats->{words}}) {
  #  my $cnt_results = $stats->{results}{$word};
  #  my $cnt_freq = $stats->{words}{$word};
  #  printf "%s: %d, %d\n", $word, $cnt_freq, $cnt_results;
  #}

  for my $word (sort {$stats->{tfidf3}{$b} <=> $stats->{tfidf3}{$a} } keys %{$stats->{tfidf3}}) {
    #my $cnt_results = $stats->{results}{$word};
    #my $cnt_freq = $stats->{words}{$word};
    printf "------ %s\n%.2f, %.2f, %s\n\n", $word,  $stats->{tfidf3}{$word}
    ,  $stats->{tfidf0}{$word}
    ,  $stats->{sample}{$word};

  }
});
