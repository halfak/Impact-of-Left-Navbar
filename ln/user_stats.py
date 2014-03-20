# Requires python3
import argparse, sys, getpass
from itertools import chain

from mw import database
from mw import Timestamp
from mw.lib import sessions
from mw.lib import reverts

from menagerie.formatting import tsv

HEADERS = [
	"day_revisions",
	"day_deleted_revisions",
	"day_main_revisions",
	"day_deleted_main_revisions",
	"day_reverted_main_revisions",
	"day_productive_edits",
	"week_revisions",
	"week_deleted_revisions", 
	"week_main_revisions",
	"week_deleted_main_revisions",
	"week_reverted_main_revisions",
	"week_productive_edits",
	"week_sessions",
	"week_session_seconds"
]

def parse_users(f):
	if not f.isatty():
		return tsv.Reader(f, types=[int, str, str])

def main():
	
	parser = argparse.ArgumentParser(
		description = "Takes a list of users and the data necessary to make " + 
		              "judgements about productivity",
		conflict_handler="resolve"
	)
	parser.add_argument(
		"--users",
		help="Input file containing a list of user_ids",
		type=lambda path: parse_users(open(path)),
		default=parse_users(sys.stdin)
	)
	parser.add_argument(
		"--revert_cutoff",
		help="How long to wait for a revert to occur? (seconds)",
		type=int,
		default=60*60*24*2 # Two days
	)
	parser.add_argument(
		"--revert_radius",
		help="How many revisions can a reverting revision revert?",
		type=int,
		default=15
	)
	parser.add_argument(
		"--no-headers",
		help="Skip printing the headers",
		action="store_true"
	)
	
	database.DB.add_args(
		parser, 
		{
			'database': "enwiki", 
			'host': "s1-analytics-slave.eqiad.wmnet",
			'user': "research"
		}
	)
	
	args = parser.parse_args()
	
	db = database.DB.from_args(args)
	
	
	
	run(db, args.users, args.revert_cutoff, args.revert_radius, args.no_headers)

def run(db, users, revert_cutoff, revert_radius, no_headers):
	
	if no_headers:
		headers=None
	else:
		headers=HEADERS
	
	output = tsv.Writer(sys.stdout, headers=headers)
	
	for user in users:
		sys.stderr.write("{0}, {1}: ".format("enwiki", user.user_id))
		
		day_revisions = 0
		day_deleted_revisions = 0
		day_main_revisions = 0
		day_deleted_main_revisions = 0
		day_reverted_main_revisions = 0
		day_productive_edits = 0
		week_revisions = 0
		week_deleted_revisions = 0 
		week_main_revisions = 0
		week_deleted_main_revisions = 0
		week_reverted_main_revisions = 0
		week_productive_edits = 0
		week_sessions = 0
		week_session_seconds = 0
		
		registration = Timestamp(user.user_registration)
		end_of_life = Timestamp(int(registration) + 60*60*24*7) # One week
		
		user_revisions = db.all_revisions.query(
			user_id=user.user_id,
			direction="newer",
			before=end_of_life,
			include_page=True
		)
		
		user_events = chain(
			[(user.user_id, registration, ('registration', registration, None))],
			(
				(
					rev['rev_user'], 
					rev['rev_timestamp'], 
					('revision', Timestamp(rev['rev_timestamp']), rev)
				)
				for rev in user_revisions
			)
		)
		
		for _, events in sessions.sessions(user_events):
			
			for event_type, timestamp, payload in events:
				
				if event_type == "revision":
					rev = payload
					day = Timestamp(rev['rev_timestamp']) - registration <= 60*60*24 # one day
					
					week_revisions += 1
					day_revisions += day
					
					week_deleted_revisions += rev['archived']
					day_deleted_revisions += rev['archived'] * day
					
					if rev['page_namespace'] == 0:
						week_main_revisions += 1
						day_main_revisions += day
						
						rev_timestamp = Timestamp(rev['rev_timestamp'])
						cutoff_timestamp = Timestamp(int(rev_timestamp) + revert_cutoff)
						
						if rev['archived']:
							week_deleted_main_revisions += 1
							day_deleted_main_revisions += day
							sys.stderr.write("a")
						else:
							revert = reverts.database.check(
								db, rev_id=rev['rev_id'], sha1=rev['rev_sha1'],
								page_id=rev['page_id'],
								radius=revert_radius,
								before=int(Timestamp(rev['rev_timestamp'])) - revert_cutoff
							)
							
							if revert != None: # Reverted edit!
								week_reverted_main_revisions += 1
								day_reverted_main_revisions += day
								sys.stderr.write("r")
							else:
								day_productive_edits += day
								week_productive_edits += 1
								sys.stderr.write(".")
					else:
						sys.stderr.write("_")
				
			
			week_sessions += 1
			week_session_seconds += events[-1][1] - events[0][1]
			
			
		
		sys.stderr.write("\n")
		output.write([
			user.user_id,
			day_revisions,
			day_deleted_revisions,
			day_main_revisions,
			day_deleted_main_revisions,
			day_reverted_main_revisions,
			day_productive_edits,
			week_revisions,
			week_deleted_revisions, 
			week_main_revisions,
			week_deleted_main_revisions,
			week_reverted_main_revisions,
			week_productive_edits,
			week_sessions,
			week_session_seconds
		])
	
