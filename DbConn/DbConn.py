#!/usr/bin/env python
import MySQLdb
import MySQLdb.cursors 
import _mysql_exceptions
import sys
import DbInsertQueue

class DbConn(object):
  '''
  Simple database connection class to reconnect to MySQL if the connection times out.
  '''
  def __init__(self, username, password, database):
    self.username = username
    self.password = password
    self.database = database
    self._conn = self._cursor = None
    self.clearParams()

  @property
  def conn(self):
    if self._conn is None:
      self.connect()
    return self._conn

  @property
  def cursor(self):
    if self._cursor is None:
      self._cursor = self.conn.cursor()
    return self._cursor

  def resetCursor(self, cursor=None):
    if cursor is None:
      try:
        self._cursor.close()
        self._cursor = None
      except AttributeError:
        # cursor hasn't been set.
        pass
    else:
      cursor.close()
      cursor = self.conn.cursor()
    return self

  def clearParams(self):
    """
    Clears query parameters.
    """
    self._type = "SELECT"
    self._table = self._order = self._group = self._start = self._limit = self._ignore = self._duplicateUpdate = None
    self._fields = []
    self._joins = []
    self._sets = []
    self._wheres = []
    self._values = []
    self._params = []
    return self

  def connect(self):
    try:
      self._conn = MySQLdb.connect('localhost', self.username, self.password, self.database, charset="utf8", use_unicode=True, cursorclass=MySQLdb.cursors.SSDictCursor)
      self._cursor = None
    except MySQLdb.Error, e:
      print "Error connecting to MySQL database %d: %s to database: %s" % (e.args[0],e.args[1],self.database)
      raise
    return True

  def select(self):
    self._type = "SELECT"
    return self

  def table(self, table):
    self._table = "".join(["`", table, "`"])
    return self

  def fields(self, *args):
    self._fields.extend(args)
    return self

  def join(self, join, joinType="INNER"):
    self._joins.append(" ".join([joinType, "JOIN", join]))
    return self

  def set(self, *args, **kwargs):
    for entry in args:
      self._sets.append(entry)
    for field, value in kwargs.items():
      if isinstance(value, (basestring, int, float, long, bool)) or value is None:
        # if scalar, assume it's a direct equals.
        self._sets.append("".join([field, " = %s"]))
        self._params.extend([value])
      else:
        raise _mysql_exceptions.InterfaceError("Non-scalar value passed to set()")
    return self

  def where(self, *args, **kwargs):
    for entry in args:
      if isinstance(entry, (list, tuple)):
        # user has provided entry in the form
        # ("UPPER(`name`) = %s", topic.name)
        self._wheres.append(entry[0])
        self._params.extend([entry[1]])
      else:
        # user has provided entry in the form
        # "UPPER(name) = 'MYNAME'"
        self._wheres.append(entry)
    for field, value in kwargs.items():
      if isinstance(value, (basestring, int, float, long, bool)):
        # if scalar, assume it's a direct equals.
        self._wheres.append("".join([field, " = %s"]))
        self._params.extend([value])
      else:
        # if not scalar, assume it's an IN query.
        self._wheres.append("".join([field, " IN (", ",".join(["%s"] * len(value)), ")"]))
        self._params.extend(value)
    return self

  def values(self, values):
    # for INSERT INTO queries.
    for entry in values:
      self._values.append("".join(["(", ",".join(["%s"] * len(entry)) , ")"]))
      self._params.extend(entry)
    return self

  def match(self, fields, query):
    # WHERE MATCH(fields) AGAINST(query IN BOOLEAN MODE)
    self._wheres.append("MATCH(" + ",".join(fields) + ") AGAINST(%s IN BOOLEAN MODE)")
    self._params.extend([query])
    return self

  def group(self, field):
    self._group = field if isinstance(field, list) else [field]
    return self

  def order(self, order):
    self._order = order
    return self

  def start(self, start):
    self._start = start
    return self

  def limit(self, limit):
    self._limit = limit
    return self

  def onDuplicateKeyUpdate(self, update):
    self._duplicateUpdate = update
    return self

  def queryString(self):
    fields = ["*"] if not self._fields else self._fields

    queryList = [self._type]

    if self._type == "SELECT":
      queryList.extend([",".join(fields), "FROM"])
    elif self._type == "INSERT":
      if self._ignore:
        queryList.append("IGNORE")
      queryList.append("INTO")
    elif self._type == "DELETE":
      queryList.append("FROM")
    elif self._type == "TRUNCATE":
      queryList.append("TABLE")
    queryList.extend([self._table, " ".join(self._joins)])

    if self._type == "INSERT":
      queryList.extend(["(" + ",".join(fields) + ")", "".join(["VALUES ", ",".join(self._values) if self._values else "()"])])
    elif self._type == "UPDATE":
      queryList.extend(["SET", ", ".join(self._sets)] if self._sets else "")

    if self._duplicateUpdate is not None:
      queryList.extend(["ON DUPLICATE KEY UPDATE", self._duplicateUpdate])

    limitString = ""
    if self._start is not None:
      limitString = "LIMIT %s, %s"
    elif self._limit is not None:
      limitString = "LIMIT %s"

    queryList.extend([" ".join(["WHERE", " && ".join(self._wheres)]) if self._wheres else "", 
                     " ".join(["GROUP BY", ",".join(self._group)]) if self._group else "", 
                     " ".join(["ORDER BY", str(self._order)]) if self._order else "", 
                     limitString])

    try:
      searchQuery = " ".join(queryList)
    except Exception as e:
      raise type(e)(e.message + ": " + str(queryList))
    return searchQuery

  def query(self, newCursor=False):
    if self._start is not None:
      if self._limit is None:
        self._params.extend([int(self._start), 18446744073709551615])
      else:
        self._params.extend([int(self._start), int(self._limit)])
    elif self._limit is not None:
      self._params.append(int(self._limit))
    try:
      try:
        if newCursor:
          cursor = self.conn.cursor()
        else:
          cursor = self.cursor
        cursor.execute(self.queryString(), self._params)
      except (AttributeError, MySQLdb.OperationalError):
        # lost connection. reconnect and re-query.
        if not self.connect():
          print "Unable to reconnect to MySQL."
          raise
        cursor = self.cursor
        cursor.execute(self.queryString(), self._params)
    except _mysql_exceptions.Error as e:
      if self._table:
        raise type(e), type(e)(unicode(e) + '\nQuery: %s\nParams: %s' % (self.queryString(), unicode(self._params))), sys.exc_info()[2]
      else:
        raise type(e), type(e)(unicode(e)), sys.exc_info()[2]
    self.clearParams()
    return cursor

  def update(self, newCursor=False, commit=True):
    self._type = "UPDATE"
    cursor = self.query(newCursor=True)
    if cursor and commit:
      self.commit()
    return cursor

  def delete(self, newCursor=False, commit=True):
    self._type = "DELETE"
    cursor = self.query(newCursor=True)
    if cursor and commit:
      self.commit()
    return cursor

  def insert(self, ignore=False, newCursor=False, commit=True):
    self._type = "INSERT"
    cursor = self.query(newCursor=True)
    if cursor and commit:
      self.commit()
    return cursor

  def truncate(self, newCursor=False, commit=True):
    self._type = "TRUNCATE"
    cursor = self.query(newCursor=True)
    if cursor and commit:
      self.commit()
    return cursor

  def list(self, valField=None, newCursor=False):
    queryCursor = self.query(newCursor=newCursor)
    if not queryCursor:
      return False
    resultList = [result[valField] for result in queryCursor] if isinstance(valField, basestring) else [result for result in queryCursor]
    if newCursor:
      self.resetCursor(cursor=queryCursor)
    else:
      self.resetCursor()
    return resultList

  def dict(self, keyField=None, valField=None, newCursor=False):
    if valField is None:
      return False
    if keyField is None:
      keyField = u'id'
    queryResults = {}
    queryCursor = self.query(newCursor=newCursor)
    if not queryCursor:
      return False
    for row in queryCursor:
      queryResults[row[keyField]] = row[valField]
    if newCursor:
      self.resetCursor(cursor=queryCursor)
    else:
      self.resetCursor()
    return queryResults

  def firstRow(self, newCursor=False):
    self._type = "SELECT"
    queryCursor = self.limit(1).query(newCursor=newCursor)
    if not queryCursor:
      return False
    firstRow = queryCursor.fetchone()
    if newCursor:
      self.resetCursor(cursor=queryCursor)
    else:
      self.resetCursor()
    return firstRow

  def firstValue(self, newCursor=False):
    firstRow = self.firstRow(newCursor=newCursor)
    if not firstRow:
      return False
    rowKeys = firstRow.keys()
    return firstRow[rowKeys[0]]

  def commit(self):
    self.conn.commit()
    return self

  def rollback(self):
    self.conn.rollback()
    return self

  def close(self):
    self.conn.close()
    self._conn = None

  def insertQueue(self, table, fields, maxLength=1000):
    return DbInsertQueue.DbInsertQueue(self, table, fields, maxLength=maxLength)