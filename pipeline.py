import time
import os
import os.path
import shutil
import glob

from seesaw.project import *
from seesaw.config import *
from seesaw.item import *
from seesaw.task import *
from seesaw.pipeline import *
from seesaw.externalprocess import *
from seesaw.tracker import *

DATA_DIR = "data"
USER_AGENT = "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27"
VERSION = "20120809.01"

class PrepareDirectories(SimpleTask):
  def __init__(self):
    SimpleTask.__init__(self, "PrepareDirectories")

  def process(self, item):
    item_name = item["item_name"]
    prefix_dir = "/".join(( DATA_DIR, item_name[0:1], item_name[0:2], item_name[0:3] ))
    dirname = "/".join(( prefix_dir, item_name ))

    if os.path.isdir(dirname):
      shutil.rmtree(dirname)

    os.makedirs(dirname + "/files")

    item["item_dir"] = dirname
    item["prefix_dir"] = prefix_dir
    item["warc_file_base"] = "tumblr-%s-%s" % (item_name, time.strftime("%Y%m%d-%H%M%S"))

class MoveFiles(SimpleTask):
  def __init__(self):
    SimpleTask.__init__(self, "MoveFiles")

  def process(self, item):
    os.rename("%(item_dir)s/%(warc_file_base)s.warc.gz" % item,
              "%(prefix_dir)s/%(warc_file_base)s.warc.gz" % item)

    shutil.rmtree("%(item_dir)s" % item)

class DeleteFiles(SimpleTask):
  def __init__(self):
    SimpleTask.__init__(self, "DeleteFiles")

  def process(self, item):
    os.unlink("%(prefix_dir)s/%(warc_file_base)s.warc.gz" % item)

def calculate_item_id(item):
  post_htmls = glob.glob("%(item_dir)s/files/%(item_name)s/post/*" % item)
  n = len(post_htmls)
  if n == 0:
    return "null"
  else:
    return post_htmls[0] + "-" + post_htmls[n-1]


project = Project(
  title = "Tumblr",
  project_html = """
    <img class="project-logo" alt="Tumblr logo" src="http://archiveteam.org/images/c/cd/TumblrLogo.png" height="50" />
    <h2>Tumblr <span class="links"><a href="https://www.tumblr.com/">Website</a> &middot; <a href="http://tracker.archiveteam.org/tumblr/">Leaderboard</a></span></h2>
    <p>Archive Tumblr blogs, a useful test case for the ArchiveTeam Warrior.</p>
  """
)

pipeline = Pipeline(
  GetItemFromTracker("http://tracker.archiveteam.org/tumblr", downloader),
  PrepareDirectories(),
  WgetDownload([ "./wget",
      "-U", USER_AGENT,
      "-nv",
      "-o", ItemInterpolation("%(item_dir)s/wget.log"),
      "--directory-prefix", ItemInterpolation("%(item_dir)s/files"),
      "--force-directories",
      "-e", "robots=off",
      "--recursive", "--level", "inf",
      "--page-requisites", "--span-hosts",
      "--adjust-extension",
      "--accept-regex", ItemInterpolation("^https?://(([0-9]+\.media|assets|media)\.tumblr\.com|s3\.amazonaws\.com|%(item_name)s)"),
      "--warc-file", ItemInterpolation("%(item_dir)s/%(warc_file_base)s"),
      "--warc-header", "operator: Archive Team",
      "--warc-header", "tumblr-dld-script-version: " + VERSION,
      "--warc-header", ItemInterpolation("tumblr-user: %(item_name)s"),
      ItemInterpolation("http://%(item_name)s/")
    ],
    max_tries = 2,
    accept_on_exit_code = [ 0, 6, 8 ],
  ),
  PrepareStatsForTracker(
    defaults = { "downloader": downloader, "version": VERSION },
    file_groups = {
      "blog": [ ItemInterpolation("%(item_dir)s/%(warc_file_base)s.warc.gz") ]
    },
    id_function = calculate_item_id
  ),
  MoveFiles(),
  LimitConcurrent(1,
    RsyncUpload(
      target = ConfigInterpolation("fos.textfiles.com::tumblr/%s/", downloader),
      target_source_path = ItemInterpolation("%(prefix_dir)s/"),
      files = [
        ItemInterpolation("%(warc_file_base)s.warc.gz")
      ],
      extra_args = [
        "--partial-dir", ".rsync-tmp"
      ]
    ),
  ),
  SendDoneToTracker(
    tracker_url = "http://tracker.archiveteam.org/tumblr",
    stats = ItemValue("stats")
  ),
  DeleteFiles()
)

