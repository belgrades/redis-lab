"""Utility to dump and load keys from Redis. Key values are encoded in JSON. In
this module the following functions are available:

  * bindump(fn, compress, match)
  * binload(fn, compress)

"""
from redis import StrictRedis
import csv

def bindump(redis, filename="/data/ru101.csv", match="*"):
  """Dump matching keys using DUMP command into file"""
  count = 0
  try:
    with open(filename, 'wb') as csvfile:
      csvwriter = csv.writer(csvfile, delimiter=',',
                             quotechar='"', quoting=csv.QUOTE_MINIMAL)
      for key in redis.scan_iter(match):
        print redis.dump(key)
        csvwriter.writerow([key, redis.type(key), redis.ttl(key), 
                            redis.dump(key)])
        count += 1
  finally:
    csvfile.close()
    print "total keys dumped: {}".format(count)

def binload(redis, filename="/data/ru101.csv"):
  """Load using RESTORE keys from file previously DUMPed"""
  def workaround_973(key, value, replace):
    """Workaround restore() validates that TTL >= 0."""
    import sys
    p = redis.pipeline()
    p.restore(key, sys.maxint, value, replace)
    p.persist(key)
    p.execute()

  count = 0
  try:
    with open(filename, 'rb') as csvfile:
      csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
      for row in csvreader:
        print row
        # Workaround for https://github.com/andymccurdy/redis-py/issues/973
        if row[2] >= 0:
          redis.restore(row[0], row[2], row[3], True)
        else:
          workaround_973(row[0], row[3], True)
        count += 1
  finally:
    csvfile.close()
    print "total keys loaded: {}".format(count)

def main(command, datafile):
  """Entry point to execute eithe rthe dump or load"""
  import os
  redis_c = StrictRedis(host=os.environ.get("REDIS_HOST", "localhost"),
                        port=os.environ.get("REDIS_PORT", 6379),
                        db=0)
  if command == "load":
    binload(redis_c, filename=datafile)
  elif command == "dump":
    dindump(redis_c, filename=datafile)
  else:
    print "Don't know how to do {}".format(command)

if __name__ == "__main__":
  main(sys.argv[1], sys.argv[2])


