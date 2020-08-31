from a01_sele import *


tag = 'SNIS-900'


if __name__ == '__main__':

    search_st = '%{}%'.format(tag)
    statement = jav.select().with_for_update().where(jav.columns.name.like(search_st)). \
        order_by(jav.columns['update time'].desc()).limit(1)

    logger.info('Connecting DB')
    with engine.connect() as con:
        result = con.execute(statement)

        # href, name, status, duration
        rs = [row for row in result]

        if len(rs) > 0:
            logger.info('Crawl Initializing ')
        else:
            logger.error('No New Video To Crawl')
            sys.exit(0)

        rs = rs[0]
        update_st = jav.update().where(jav.columns.name == rs[1]). \
            values({'status': 'P', 'update time': str(datetime.datetime.now())})
        result = con.execute(update_st)

    logger.info('Now Crawling Name : {}'.format(rs[1]))
    logger.info('URL : {}'.format(rs[0]))
    size = math.ceil(rs[6] / 4)
    v = VideoCatch(rs[0], rs[1], size)
    v.run()
