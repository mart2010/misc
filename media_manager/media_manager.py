import os
import glob
import shutil
from datetime import date, datetime
import platform
import logging
import argparse
# pip install Pillow
from PIL import Image, UnidentifiedImageError
from PIL.ExifTags import TAGS
# pip install hachoir
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata

MEDIA_FILES_EXT =  dict(image=('jpeg', 'jpg', 'heic', 'bmp', 'png', 'gif', ), 
                        video=('avi', 'mp4', 'mov', 'hevc', '3gp', 'mkv', 'm4v'))

# possible exif tag-ids for Image creation-date 
EXIF_DATE_TAGS = dict(DateTimeOriginal=36867, 
                      DateTime=306)
VID_DATE_META = 'creation date'

logger = None

def file_creation_date(filepath):
    """
    Return  file creation date, falling back to when it was last modified.
    See http://stackoverflow.com/a/39501288/1709587
    """
    c_timestamp = None
    fstat = os.stat(filepath)
    if platform.system() == 'Windows':
        c_timestamp = fstat.st_ctime
    else:
        try:
            c_timestamp = fstat.st_birthdate
        except AttributeError:
            # typically on Linux, only possible to get 'last modified'
            c_timestamp = fstat.st_mtime
    if c_timestamp:
        return datetime.fromtimestamp(c_timestamp)
    else:
        raise Exception(f"Could not find creation date of {filepath!r}")

def image_creation_date(img_filepath):
    """Return image creation date from image's header metadata 
    """
    found_date = datetime.max
    date_pattern = '%Y:%m:%d %H:%M:%S'

    try: 
        with Image.open(img_filepath) as img:
            img_exif = img.getexif()
    except UnidentifiedImageError as e:
        logger.error(e)
    else:
        if img_exif:
            all_tags = [f"{TAGS.get(k,'Unknonw tag')}({k}): {v}({v.__class__.__name__})" for k,v in img_exif.items()] 
            logger.debug(f"Exif metadata for {img_filepath!r}:\n{' | '.join(all_tags)}")
            for n,k in EXIF_DATE_TAGS.items():
                try:
                    extracted_date = datetime.strptime(img_exif[k],date_pattern) if img_exif.get(k) else datetime.max
                except ValueError:
                    logger.error(f"Unrecognized Date {img_exif[k]!r} extracted from ExifTAGS {n!r}")
                else:
                    if extracted_date < found_date:
                        found_date = extracted_date
    if found_date == datetime.max:
        return None
    else:
        return found_date


def video_creation_date(mov_filepath):
    parser = createParser(mov_filepath)
    if not parser:
        logger.info(f"Could not parse video file {mov_filepath!r} ")
        return None
    with parser:
        metadata = extractMetadata(parser)
    if not metadata:
        logger.info(f"Could not extract metadata of video file {mov_filepath!r}")
        return None
    
    all_meta =  metadata.exportPlaintext() 
    logger.debug(f"Video metadata for {mov_filepath!r}:\n{' | '.join(all_meta)}")
    for line in all_meta:
        meta_l = line.split(':')
        if VID_DATE_META in meta_l[0].lower(): 
            date_comp_str = meta_l[1].split()[0]
            return datetime.strptime(date_comp_str, "%Y-%m-%d")
    return None

def derive_media_date(media_file, media_type):
    found_date = None
    # 1- attempt to get date from metadata/header
    if media_type == 'image':
        found_date = image_creation_date(media_file)
    else:
        found_date = video_creation_date(media_file)
    
    # 2- fall back using file creation date (OS)
    if not found_date:
        found_date = file_creation_date(media_file)
    return found_date

def yield_media_files(src_dir, media_type):
    """Generate tuple(media-filepath, creation_date)
    from files found recursively in source_dir with file_exts
    """
    files_ext = MEDIA_FILES_EXT[media_type]

    # for non-Windows, include case-sensitive extension 
    if platform.system () != 'Windows':
        files_ext = [e.upper() if i<len(files_ext) else e.lower() for i,e in enumerate(files_ext*2) ]

    file_patterns = [os.path.join(src_dir, "**", f"*.{e}") for e in files_ext]

    for p in file_patterns:
        ctn = 0 
        for m_file in glob.iglob(f"{p}", recursive=True):
            ctn += 1
            yield((m_file, derive_media_date(m_file, media_type=media_type)))
        logger.info(f"Found {ctn} media files matching {p!r}")


def move_media_files(src_dir, tgt_dir, media_type, dir_pattern, keep_original, overwrite, media_subdir=False):
    tgt_dir = os.path.abspath(tgt_dir)
    src_dir = os.path.abspath(src_dir)
    if not os.path.exists(src_dir):
        raise Exception(f"Source dir {src_dir!r} does not exist")

    for m_file, m_date in yield_media_files(src_dir, media_type=media_type):
        subdir = f"{m_date.strftime(dir_pattern)}"
        if media_subdir:
            tgt_filepath = os.path.join(tgt_dir, subdir, media_type.capitalize(), os.path.basename(m_file))
        else:
            tgt_filepath = os.path.join(tgt_dir, subdir, os.path.basename(m_file))
        
        if not os.path.exists(os.path.dirname(tgt_filepath)):
            os.makedirs(os.path.dirname(tgt_filepath))
        else:
            if os.path.exists(tgt_filepath) and not overwrite:
                logger.info(f"Target File exists: {tgt_filepath!r}")
                continue
        
        shutil.copy2(m_file, tgt_filepath)

        if not keep_original:
            logger.debug(f"Deleting original file: {m_file!r}")
            os.remove(m_file)
            
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('src_dir', help="Source directory to scan for media files")
    parser.add_argument('tgt_dir', default='.', help="Target directory to move/copy files into (default=current)")
    parser.add_argument('-m', '--media_type', choices=('image','video','all'), default='all', help="Media type to scan")

    parser.add_argument('-d','--dir_pattern', default="%Y", choices=("%Y", "%Y-%m", "%Y-%m-%d"), help="Directory template date name")
    parser.add_argument('-k', '--keep_ori', action='store_true', help="Keep original media file")
    parser.add_argument('-o', '--overwrite', action='store_true', help="Overwrite when target file is present")
    parser.add_argument('-log', '--loglevel', default=logging._nameToLevel['WARNING'], choices=logging._nameToLevel.keys(), help="Provide loggin level")
    args = parser.parse_args()
    print(args)

    # logging minimum setup
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(args.loglevel)

    if args.media_type in ('all','image'):
        move_media_files(args.src_dir, args.tgt_dir, 'image', args.dir_pattern, args.keep_ori, args.overwrite)
    if args.media_type in ('all','video'):
        move_media_files(args.src_dir, args.tgt_dir, 'video', args.dir_pattern, args.keep_ori, args.overwrite)
        


