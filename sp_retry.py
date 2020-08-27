from a01_sele import VideoCatch, folder_path

sub_folder = 'AVSA-138'

if __name__ == '__main__':

    v = VideoCatch('dummy', sub_folder, 10)

    v._post_merge_file()

    v._post_update_status()

    v._post_scp()
