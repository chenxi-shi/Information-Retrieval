#!/usr/bin/perl -w

# Name:	  trec_eval
#
# Who:	  Javed Aslam
# When:	  01/16/02

# Usage


if (@ARGV < 2 || @ARGV > 3) {
  die "Usage:  trec_eval [-q] <qrel_file> <trec_file>\n\n";
  }

# Get names of qrel and trec files; check for -q option.

if (@ARGV == 3) {
  shift;				# Remove -q.
  $print_all_queries = 1;
  }

$qrel_file = shift;			# Shift implicitly acts on @ARGV.
$trec_file = shift;

# Process qrel file first.

open(QREL, $qrel_file) or
  die "Failed to open $qrel_file: $!\n\n";

{
local $/ = undef;			# Reads grab the whole file.
@data = split(/\s+/, <QREL>);		# Data array has all values from the
}					# file consecutively.

close(QREL) or
  die "Couldn't close $qrel_file: $!\n\n";

# Now take the values from the data array (four at a time) and
# put them in a data structure.  Here's how it will work.
#
# %qrel is a hash whose keys are topic IDs and whose values are
# references to hashes.  Each referenced hash has keys which are
# doc IDs and values which are relevance values.  In other words...
#
# %qrel				The qrel hash.
# $qrel{$topic}			Reference to a hash for $topic.
# $qrel{$topic}->{$doc_id}	The relevance of $doc_id in $topic.
#
# %num_rel			Hash whose values are (expected) number
#				of docs relevant for each topic.

$dummy = 0;                             # A dummy variable...

while (($topic, $dummy, $doc_id, $rel) = splice(@data,0,4)) {
  $qrel{$topic}->{$doc_id} = $rel;
  $num_rel{$topic} += $rel;
  }

# The following code snippet tests this data structure.
#
# while (($topic, $href) = each(%qrel)) {
#   while (($doc_id, $rel) = each(%$href)) {
#     print "$topic $doc_id $rel\n";
#     }
#   }

# Now process the trec file.

open(TREC, $trec_file) or
  die "Failed to open $trec_file: $!\n\n";

{
local $/ = undef;			# Reads grab the whole file.
@data = split(/\s+/, <TREC>);		# Data array has all values from the
}					# file consecutively.

close(TREC) or
  die "Couldn't close $qrel_file: $!\n\n";

# Process the trec_file data in much the same manner as above.

while (($topic, $dummy, $doc_id, $dummy, $score, $dummy) = splice(@data,0,6)) {
  $trec{$topic}->{$doc_id} = $score;
  }

# The following code snippet tests this data structure.
#
# while (($topic, $href) = each(%trec)) {
#   while (($doc_id, $score) = each(%$href)) {
#     print "$topic $doc_id $score\n";
#     }
#   }

# Initialize some arrays.

@recalls = (0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0);
@cutoffs = (5, 10, 15, 20, 30, 100, 200, 500, 1000);

# Now let's process the data from trec_file to get results.

foreach $topic (sort keys %trec) {	# Process topics in order.

  if (!$num_rel{$topic}) {              # If no relevant docs, skip topic.
    next;
    }

  $num_topics++;			# Processing another topic...
  $href = $trec{$topic};		# Get hash pointer.

  @prec_list = ();                      # New list of precisions.
  $#prec_list = 1000;			# Last index is 1000.
  @rec_list = ();                       # Recall list.
  $#rec_list = 1000;                    # Last index is 1000.

  $num_ret = 0;                         # Initialize number retrieved.
  $num_rel_ret = 0;                     # Initialize number relevant retrieved.
  $sum_prec = 0;                        # Initialize sum precision.

  # Now sort doc IDs based on scores and calculate stats.
  # Note:  Break score ties lexicographically based on doc IDs.
  # Note2: Explicitly quit after 1000 docs to conform to TREC while still
  #        handling trec_files with possibly more docs.

  foreach $doc_id (sort
    { ($href->{$b} <=> $href->{$a}) || ($b cmp $a) } keys %$href) {

    $num_ret++;                         # New retrieved doc.
    $rel = $qrel{$topic}->{$doc_id};	# Doc's relevance.

    if ($rel) {
      $sum_prec += $rel * (1 + $num_rel_ret) / $num_ret;
      $num_rel_ret += $rel;
      }

    $prec_list[$num_ret] =  $num_rel_ret/$num_ret;
    $rec_list[$num_ret] =  $num_rel_ret/$num_rel{$topic};

    if ($num_ret >= 1000) {
      last;
      }

    }

  $avg_prec = $sum_prec/$num_rel{$topic};

  # Fill out the remainder of the precision/recall lists, if necessary.

  $final_recall = $num_rel_ret/$num_rel{$topic};

  for ($i=$num_ret+1; $i<=1000; $i++) {
    $prec_list[$i] = $num_rel_ret/$i;
    $rec_list[$i] = $final_recall;
    }

  # Now calculate precision at document cutoff levels and R-precision.
  # Note that arrays are indexed starting at 0...

  @prec_at_cutoffs = ();

  foreach $cutoff (@cutoffs) {
    push @prec_at_cutoffs, $prec_list[$cutoff];
    }

  # Now calculate R-precision.  We'll be a bit anal here and
  # actually interpolate if the number of relevant docs is not
  # an integer...

  if ($num_rel{$topic} > $num_ret) {
    $r_prec = $num_rel_ret/$num_rel{$topic};
    }
  else {

    $int_num_rel = int($num_rel{$topic});                # Integer part.
    $frac_num_rel = $num_rel{$topic} - $int_num_rel;     # Fractional part.

    $r_prec = ($frac_num_rel > 0) ?
              (1 - $frac_num_rel) * $prec_list[$int_num_rel] + 
                $frac_num_rel * $prec_list[$int_num_rel+1] :
              $prec_list[$int_num_rel];

    }

  # Now calculate interpolated precisions...

 $max_prec = 0;
 for ($i=1000; $i>=1; $i--) {
   if ($prec_list[$i] > $max_prec) {
     $max_prec = $prec_list[$i];
     }
   else {
     $prec_list[$i] = $max_prec;
     }
   }

  # Finally, calculate precision at recall levels.

  @prec_at_recalls = ();

  $i = 1;
  foreach $recall (@recalls) {
    while ($i <= 1000 && $rec_list[$i] < $recall) {
      $i++
      }
    if ($i <= 1000) {
      push @prec_at_recalls, $prec_list[$i];
      }
    else {
      push @prec_at_recalls, 0;
      }
    }

  # Print stats on a per query basis if requested.

  if ($print_all_queries) {
    eval_print($topic, $num_ret, $num_rel{$topic}, $num_rel_ret,
               @prec_at_recalls, $avg_prec, @prec_at_cutoffs, $r_prec);
    }

  # Now update running sums for overall stats.

  $tot_num_ret += $num_ret;
  $tot_num_rel += $num_rel{$topic};
  $tot_num_rel_ret += $num_rel_ret;

  for ($i=0; $i<@cutoffs; $i++) {
    $sum_prec_at_cutoffs[$i] += $prec_at_cutoffs[$i];
    }

  for ($i=0; $i<@recalls; $i++) {
    $sum_prec_at_recalls[$i] += $prec_at_recalls[$i];
    }

  $sum_avg_prec += $avg_prec;
  $sum_r_prec += $r_prec;

  }

# Now calculate summary stats.

for ($i=0; $i<@cutoffs; $i++) {
  $avg_prec_at_cutoffs[$i] = $sum_prec_at_cutoffs[$i]/$num_topics;
  }

for ($i=0; $i<@recalls; $i++) {
  $avg_prec_at_recalls[$i] = $sum_prec_at_recalls[$i]/$num_topics;
  }

$mean_avg_prec = $sum_avg_prec/$num_topics;
$avg_r_prec = $sum_r_prec/$num_topics;

eval_print($num_topics, $tot_num_ret, $tot_num_rel, $tot_num_rel_ret,
           @avg_prec_at_recalls, $mean_avg_prec, @avg_prec_at_cutoffs, 
           $avg_r_prec);


##
## Subroutines.
##


sub eval_print {
  my ($qid,$ret,$rel,$rel_ret,
      $p0,$p1,$p2,$p3,$p4,$p5,$p6,$p7,$p8,$p9,$p10,
      $map,
      $p5d,$p10d,$p15d,$p20d,$p30d,$p100d,$p200d,$p500d,$p1000d,
      $rp) = @_;

  printf "\nQueryid (Num):    %5d\n", $qid;
  printf "Total number of documents over all queries\n";
  printf "    Retrieved:    %5d\n", $ret;
  printf "    Relevant:     %5d\n", $rel;
  printf "    Rel_ret:      %5d\n", $rel_ret;
  printf "Interpolated Recall - Precision Averages:\n";
  printf "    at 0.00       %.4f\n", $p0;
  printf "    at 0.10       %.4f\n", $p1;
  printf "    at 0.20       %.4f\n", $p2;
  printf "    at 0.30       %.4f\n", $p3;
  printf "    at 0.40       %.4f\n", $p4;
  printf "    at 0.50       %.4f\n", $p5;
  printf "    at 0.60       %.4f\n", $p6;
  printf "    at 0.70       %.4f\n", $p7;
  printf "    at 0.80       %.4f\n", $p8;
  printf "    at 0.90       %.4f\n", $p9;
  printf "    at 1.00       %.4f\n", $p10;
  printf "Average precision (non-interpolated) for all rel docs(averaged over queries)\n";
  printf "                  %.4f\n", $map;
  printf "Precision:\n";
  printf "  At    5 docs:   %.4f\n", $p5d;
  printf "  At   10 docs:   %.4f\n", $p10d;
  printf "  At   15 docs:   %.4f\n", $p15d;
  printf "  At   20 docs:   %.4f\n", $p20d;
  printf "  At   30 docs:   %.4f\n", $p30d;
  printf "  At  100 docs:   %.4f\n", $p100d;
  printf "  At  200 docs:   %.4f\n", $p200d;
  printf "  At  500 docs:   %.4f\n", $p500d;
  printf "  At 1000 docs:   %.4f\n", $p1000d;
  printf "R-Precision (precision after R (= num_rel for a query) docs retrieved):\n";
  printf "    Exact:        %.4f\n", $rp;
  }