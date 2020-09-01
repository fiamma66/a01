from a01_sele import *

sub_folder = 'SIM-085'
sp = 3144

if __name__ == '__main__':

    like_string = '%{}%'.format(sub_folder)

    main(mode='sp_retry', sp_name=like_string, chunk_num=sp)
