from time import sleep

from extract import extract_tc
from merge import merge_tc
from sqoop import pd_reverse, pd_conversion

if __name__ == '__main__':
    try:
        extract_tc.run()
        sleep(1)
        pd_conversion.run()
        sleep(1)
        merge_tc.run()
        sleep(1)
        pd_reverse.run()
        print("流程结束")
    except Exception as e:
        print(e)
