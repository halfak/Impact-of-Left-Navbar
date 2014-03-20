

datasets/users.tsv: sql/users.sql
	cat sql/users.sql | \
	mysql --defaults-file=~/.my.cnf \
		-h s1-analytics-slave.eqiad.wmnet \
		-u research > \
	datasets/users.tsv
	
datasets/user_stats.tsv: ln/user_stats.py \
                         datasets/users.tsv
	cat datasets/users.tsv | \
	./user_stats > \
	datasets/user_stats.tsv