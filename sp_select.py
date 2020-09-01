from a01_sele import *


tag = 'SNIS-900'


if __name__ == '__main__':

    search_st = '%{}%'.format(tag)
    main(mode='sp', sp_name=search_st)
