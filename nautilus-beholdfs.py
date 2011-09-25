import os
import sys
import nautilus
import gtk
import pygtk
import urllib

BASE_PATH = os.path.join(
	os.environ.get("HOME"),
	".nautilus/python-extensions")

GUI_FILE = os.path.join(
	BASE_PATH,
	"nautilus-beholdfs.glade")

TAG_CHAR = "%"

class BeholdPath:

	def __init__(self, path):
		self.path = []
		self.tags = []
		self.listing = False
		prev = None
		while path:
			if prev: self.path.insert(0, prev)

			head, tail = os.path.split(path)
			if not tail:
				self.path.insert(0, path)
				break

			if not tail.startswith(TAG_CHAR):
				path, prev = head, tail
				continue

			tail = tail[1:]

			if not tail:
				if None == prev:
					self.listing = True
					path, prev = head, tail
					continue
				tail = prev

			self.tags.append(tail)
			path, prev = head, ""
		else:
			if prev: self.path.insert(0, prev)


	def __str__(self):
		return os.path.join(*(self.path + [ TAG_CHAR + tag for tag in self.tags ]))

	def get_path(self):
		return self.path

	def get_tags(self):
		return self.tags

class BeholdPanel(nautilus.LocationWidgetProvider):

	def __init__(self):
		pass

	def toggle_tag(self, button, uri, window):
		fs = BeholdPath(uri)
		tag = button.get_label()
		tags = fs.get_tags()

		if button.get_active():
			if not tag in tags: tags.append(tag)
		else:
			if tag in tags: tags.remove(tag)

		button.set_tooltip_text(str(fs))
		#new_dir = "file://" + str(fs)
		#window.set_directory(new_dir)

	def make_error(self, text):
		return None
		# debug
		label = gtk.Label(text)
		label.show()
		return label

	def get_widget(self, uri, window):

		if not uri.startswith("file://"):
			return self.make_error("Not a file URI: %s" % uri)

		uri = urllib.url2pathname(uri[7:])
		if not os.path.isdir(uri):
			return self.make_error("Not a directory: %s" % uri)

		tags_dir = os.path.join(uri, TAG_CHAR)
		try:
			tags = os.listdir(tags_dir)
		except OSError as e:
			return self.make_error("OSError: %s (%d)" % (e.strerror, e.errno))

		# join tags from the uri itself
		fs = BeholdPath(uri)
		include = fs.get_tags()
		tags[0:0] = include

		if not tags:
			return self.make_error("There are no tags defined")

		container = gtk.HBox()
		container.set_spacing(5)
		for tag in tags:
			btn = gtk.ToggleButton(tag)
			btn.set_active(tag in include)
			btn.connect("toggled", self.toggle_tag, uri, window)
			btn.show()
			container.pack_start(btn, False)

		container.show()
		return container

