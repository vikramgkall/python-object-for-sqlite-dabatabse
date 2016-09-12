#!/usr/bin/env python

import os
import sqlite3

class db(object):

        #filename is the name of the db file	
	def __init__(self,filename):
		try:
			self.conn = sqlite3.connect(filename)
			self.conn_handle = self.conn.cursor()

			# The last argument unique(column_1, column_2, column_3, column_4) 
                        # makes sure that all the entries in the db have unique col1, col2 and col3.
			self.conn_handle.execute("CREATE TABLE if not exists TABLE_1 
                        (column_1 TEXT, column_2 TEXT, column_3 TEXT, column_4 DATETIME, 
                        column_5 DATETIME, column_6 INT, column_7 INT, column_8 INT, column_9 TEXT,
                        unique(column_1, column_2, column_3, column_4))")

                        #creating an index for faster search in the table
			self.conn_handle.execute("CREATE INDEX index_column_3 ON TABLE_1 (column_3)")
			self.conn_handle.execute('BEGIN TRANSACTION')

		except sqlite3.Error, e:
			print "sqlite error in init"
			print "sqlite error %s:" %e.args[0]
			self.close()
			exit()
			

	def close(self):
		self.conn.commit()
		self.conn_handle.close()

	# insert a log line into the database
	def insert(self,log_line):
		try:

			pattern = "?,"
			pattern = pattern * len(log_line)
			pattern = pattern[0:len(pattern)-1]
			pattern = "(" + pattern + ")"	
			#self.conn_handle.execute("INSERT OR IGNORE INTO hydra_access_log VALUES (?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?, ?)", log_line)
			self.conn_handle.execute("INSERT OR IGNORE INTO hydra_access_log VALUES " + pattern, log_line)

			# Don't commit the changes here. If changes are commited here a new transaction has
			# to be opened for inserting every single log line. It takes much longer for this 
                        # script to execute. Instead commit all changes before closing the connection to the db

			# self.conn.commit()

		except sqlite3.Error, e:
			print "sql error in insert"
			print "sqlite error %s:" %e.args[0]
			self.close()
			exit()

	def query(self,query_statement):
		try:
			rows = self.conn_handle.execute(query_statement)
			print rows
			return rows
		except sqlite3.Error, e:
                        print "sql error in query"
			print "sqlite error %s:" %e.args[0]

