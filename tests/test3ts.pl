#!perl
use lib (q{E:\work\kmg\gather});
use lib (q{J:\projects\perl\kmg\gather});
use query_graph_object;
use Data::Dumper;
use Getopt::Long qw(:config);

our $cfg={};
GetOptions($cfg, qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s));
$cfg{defragment}//=0;

  my $g = query_graph_object->new($cfg);
  my $req = {request =>"get_node_info", start=>"20160105", end=>"20160112", url=> "chrome-extension://jhfpbldkagcaijkjcklbdccomadpedae/popup/popup.html"};
  my $t = $g->get_node_info($req);
  print Dumper($t);

