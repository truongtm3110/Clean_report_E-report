#  Copyright (c) 2019 BeeCost Team <beecost.com@gmail.com>. All Rights Reserved
#  BeeCost Project can not be copied and/or distributed without the express permission of @tuantmtb
import datetime
import json


def default_encode(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        # return o.isoformat()
        return int(o.timestamp())


if __name__ == '__main__':
    item = {'ts': datetime.datetime.now()}
    print(json.dumps(
        item,
        sort_keys=True,
        indent=1,
        default=default_encode
    ))
