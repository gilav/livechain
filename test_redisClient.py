#
#
#
import sys, traceback
import logging
from notification.RedisLPublisher import RedisPublisher




if __name__ == "__main__":
	try:
		logging.basicConfig(level=logging.DEBUG,
							format='%(asctime)s - %(message)s',
							datefmt='%Y-%m-%d %H:%M:%S')
		t = RedisPublisher()
		if len(sys.argv)!=3:
			print(f"{len(sys.argv)} args passwd. Syntax test_redisClient channel data")
			sys.exit(1)
		t.publish(sys.argv[1], sys.argv[2])
		sys.exit(0)
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		print(f"Error {exc_type} {exc_obj}")
		traceback.print_exc(file=sys.stdout)
		sys.exit(2)
