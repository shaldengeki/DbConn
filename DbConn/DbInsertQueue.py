#!/usr/bin/env python
class DbInsertQueueException(Exception):
  pass

class DbInsertQueue(object):
  '''
  Simple database class to insert large volumes of entries into a table.
  '''
  def __init__(self, dbConn, table, fields, maxLength=5000):
    self.db = dbConn
    self.table(table).fields(*fields).clear().maxLength(maxLength).ignore(False).update(None)

  def __str__(self):
    return "DbInsertQueue on table " + str(self._table) + " for fields " + str(self._fields) + " with maximum length " + str(self._maxLength)

  def __unicode__(self):
    return u"DbInsertQueue on table " + unicode(self._table) + u" for fields " + unicode(self._fields) + u" with maximum length " + unicode(self._maxLength)

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

  def fields(self, *args):
    # sets fields.
    self._fields = args
    self._numFields = len(args)
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

    new_row = []
    for idx,field in enumerate(self._fields):
      if field not in row:
        raise DbInsertQueueException(u"Row field mismatch: " + unicode(field) + " not found in queued row")
      new_row.append((idx, row[field]))

    new_row = [x[1] for x in sorted(new_row, key=lambda x: x[0])]
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