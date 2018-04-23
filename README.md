# Run Selection checks results code

The script rscheck.py produces an ascii file with the results from the low-level and high-level data-quality checks. 

You will need to add to the current directory a file named settings.py. 

It can be downloaded from here: (collaboration username and password):
http://www.lip.pt/~gersende/snop/run_selection/runselection_settings.py

Run with:
python rschecks.py -n firstrun -i lastrun

For hints on how to read the column headings, see:
https://www.snolab.ca/snoplus/TWiki/bin/view/RunSelection/RunSelectionNotes

The script ratdb_tables.py produces a text file that lists runs missing critical RATDB tables within a user input run range. This is designed for run selectors to check all the necessary RATDB tables exist before they mark a run as gold.
