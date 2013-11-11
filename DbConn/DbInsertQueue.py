#!/usr/bin/env python
class DbInsertQueueException(Exception):
  pass

class DbInsertQueue(object):
  '''
  Simple database class to insert large volumes of entries into a table.
  '''
  def __init__(self, dbConn, table, fields, maxLength=1000):
    self.db = dbConn
    self.table(table).fields(fields).clear().maxLength(maxLength).ignore(False).update(None)


  def __len__(self):
    return int(self._length)

  def __iter__(self):
    for x in self._rows:
      yield x

  def clear(self):
    self._rows = []
    self._length = 0
    return self

  def table(self, table):
    self._table = unicode(table)
    return self

  def fields(self, fields):
    # sets fields.
    self._fields = list(fields)
    self._numFields = len(fields)
    return self

  def maxLength(self, maxLength):
    self._maxLength = int(maxLength)
    return self

  def ignore(self, ignore):
    self._ignore = bool(ignore)
    return self

  def update(self, update):
    self._update = update
    return self

  def queue(self, row):
    if len(row) != self._numFields:
      raise DbInsertQueueException(u"Row field count mismatch: trying to queue " + unicode(len(row)) + u" values into " + unicode(self._numFields) + u" fields")
    self._rows.append(row)
    self._length += 1
    if self._length > self._maxLength:
      self.flush()

  def flush(self):
    if self._rows:
      self.db.table(self._table).fields(*self._fields).values(self._rows)
      if self._update:
        self.db.onDuplicateKeyUpdate(self._update)
      self.db.insert(ignore=self._ignore, newCursor=True)
      self.db.commit()
    self.clear()
    return self