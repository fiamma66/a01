import a01_sele as a01

sub_folder = 'SIM-085'
sp = 3144

if __name__ == '__main__':

    statement = a01.jav.select().with_for_update().where(a01.jav.columns.name == sub_folder).\
        order_by(a01.jav.columns['update time'].desc()).limit(1)
    # logger.info('Connecting DB')
    with a01.engine.connect() as con:
        result = con.execute(statement)

    rs = [row for row in result]
    rs = rs[0]
    size = a01.math.ceil(rs[6] / 4)
    v = a01.VideoCatch(rs[0], rs[1], size)
    v.sp_rerun(sp=sp)

    # v._post_merge_file()
    #
    # v._post_update_status()
    #
    # v._post_scp()
