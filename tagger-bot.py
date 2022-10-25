import collections
import contextlib
import dataclasses
import difflib
import gettext
import json
import locale
import logging
import pathlib
import textwrap

gettext.bindtextdomain("taggerbot", "locale")
gettext.textdomain("taggerbot")

from typing import Any, Callable, Dict, List, Optional, Tuple

_ = gettext.gettext

logger = logging.getLogger(__name__)


class MissingParameterError(Exception):
    pass


class TagMapping:
    def __init__(self, key="mapping"):
        self.tags = collections.defaultdict(set)
        self.users = collections.defaultdict(set)
        self.dirty = False
        self.key = key

    def dump(self):
        return {t: list(us) for t, us in self.tags.items() if len(us) > 0}

    def load(self, storage):
        d = storage.get(self.key, {})
        self.tags.clear()
        self.users.clear()
        self.dirty = False
        for tag, users in d.items():
            self.tags[tag.lower()].update(users)
            for user in users:
                self.users[user].add(tag.lower())
        return self

    def store(self, storage):
        if self.dirty:
            storage.put(self.key, self.dump())
            self.dirty = False
        return self

    @contextlib.contextmanager
    def use(self, storage):
        self.load(storage)
        yield self
        self.store(storage)

    def add(self, user, *tags):
        self.dirty = True
        for tag in tags:
            self.tags[tag.lower()].add(user)
            self.users[user].add(tag.lower())

    def remove(self, user, *tags):
        self.dirty = True
        for tag in tags:
            self.tags[tag.lower()].discard(user)
            self.users[user].discard(tag.lower())

    def find(self, tag=None, user=None):
        # assert tag is None ^ user is None
        if tag is not None:
            return sorted(self.tags[tag.lower()])
        if user is not None:
            return sorted(self.users[user])
        raise KeyError()

    def __contains__(self, tag):
        return tag in self.tags

    def nearest(self, tag) -> Tuple[str, float]:
        return max(
            (
                (t, difflib.SequenceMatcher(a=t.lower(), b=tag.lower()).ratio())
                for t in self.tags.keys()
            ),
            key=lambda e: e[1],
        )


class StorageContainer:
    def get(self, key, default=None):
        raise NotImplementedError()

    def put(self, key, val):
        raise NotImplementedError()

    def contains(self, key):
        raise NotImplementedError()


class ZulipStorage(StorageContainer):
    def __init__(self, handler):
        self.handler = handler

    def get(self, key, default=None):
        if self.contains(key):
            return self.handler.storage.get(key)
        else:
            return default

    def put(self, key, val):
        return self.handler.storage.put(key, val)

    def contains(self, key):
        return self.handler.storage.contains(key)


class JsonFileStorage(StorageContainer):
    def __init__(self, fname):
        self.path = pathlib.Path(fname)
        if self.path.exists():
            self.data = json.load(self.path.open("r"))
        else:
            self.data = {}

    def get(self, key, default=None):
        return self.data.get(key, default)

    def put(self, key, val):
        self.data[key] = val
        json.dump(self.data, self.path.open("w"))

    def contains(self, key):
        return key in self.data


def read_parameters(params):
    if len(params) != 1:
        raise MissingParameterError()
    return set(map(str.strip, params[0].split(",")))


CommandHandler = Callable[
    [str, str, List[str], Dict[str, str], StorageContainer, Any], str
]

Command = collections.namedtuple(
    "Command", "command syntax help handler", defaults=["", "", lambda *a: None]
)


def command_help(
    sender: str,
    command: str,
    params: List[str],
    storage: StorageContainer,
    bot_handler: Any = None,
) -> str:
    return TaggerBotHandler.help_text()


class Command_Manage:
    def __init__(self, parser=lambda *a: [], mutator=lambda *a: None):
        self.parser = parser
        self.mutator = mutator

    def __call__(
        self,
        sender: str,
        command: str,
        params: List[str],
        storage: StorageContainer,
        bot_handler: Any = None,
    ) -> str:
        all_tags = self.parser(params)
        with TagMapping().use(storage) as tags:
            self.mutator(tags, sender, *all_tags)
            return _("Hi @**{}**, you are currently tagged with: {}").format(
                sender,
                ", ".join(tags.find(user=sender)),
            )


def command_search(
    sender: str,
    command: str,
    params: List[str],
    storage: StorageContainer,
    bot_handler: Any = None,
) -> str:
    all_tags = read_parameters(params)
    with TagMapping().use(storage) as tags:
        for tag in all_tags:
            if tag.lower() not in tags:
                nearest, ratio = tags.nearest(tag.lower())
                if ratio > 0.75:
                    return _("Hi @**{}**, I don't know the tag '{}'").format(
                        sender,
                        tag,
                        nearest,
                    )
                else:
                    return _(
                        "Hi @**{}**, I don't know the tag '{}' - did you mean '{}'?"
                    ).format(
                        sender,
                        tag,
                    )

        results = [tags.find(tag=tag) for tag in all_tags]
        limit = set(storage.get("limit", []))
        if len(limit) > 0:
            intersection = limit.intersection(*results)
        else:
            intersection = results[0].intersection(*results[:1])
        return _("Hi @**{}**, here's a list of everybody tagged with: {}\n\n{}").format(
            sender,
            (" " + _("and") + " ").join(all_tags),
            "\n".join("- @**{}**".format(user) for user in intersection),
        )


def command_limit(
    self,
    sender: str,
    command: str,
    params: List[str],
    storage: StorageContainer,
    bot_handler: Any = None,
) -> str:
    if command == _("limit"):
        users = read_parameters(params)
        limit = set(storage.get("limit", []))
        limit = limit.union(users)
    elif command == _("unlimit"):
        limit = []
    storage.put("limit", list(limit))
    if len(limit) > 0:
        return _("Tag search is currently limited to: {}").format(", ".join(limit))
    else:
        return _("Tag search is currently unlimited")


class TaggerBotHandler:
    """
    This plugin provides simple interface to store and query random tags
    provided by users.

    Data is stored using zulip's bot storage system.

    There are three index lists:
    - users: A list of all users that have any tags on them
    - tags: A set of all tags that are applied to at least one user
    - limit: A list of users that will be @mentioned.
    """

    META = {
        "name": "Tagger",
        "description": "Allows user tag store/lookup",
    }

    strings: Dict[str, str] = {}  # For i18n of all text

    @classmethod
    def build_commands(cls) -> List[Tuple[str, str, str, Command]]:
        """
        Build a dictionary of commands.

        Note: This has to be done after the configuration with i18n
        options has been loaded, otherwise the strings dictionary would
        be available.
        """
        cls.commands = [
            Command(
                _("help"),
                "",
                _("To show all commands the bot supports."),
                command_help,
            ),
            Command(
                _("list"),
                "",
                _("Show all tags currently applied to the user."),
                Command_Manage(),
            ),
            Command(
                _("add"),
                _("<tag>, <tag>, ..."),
                _("To add personal tag(s)."),
                Command_Manage(read_parameters, TagMapping.add),
            ),
            Command(
                _("remove"),
                _("<tag>, <tag>, ..."),
                _("To remove personal tag(s)."),
                Command_Manage(read_parameters, TagMapping.remove),
            ),
            Command(
                _("search"),
                _("<tag>, <tag>, ..."),
                _("To search for somebody with <tag>."),
                command_search,
            ),
            Command(
                _("limit"),
                _("<user>, <user>, ..."),
                _("Limit search to this group of users."),
                command_limit,
            ),
            Command(_("unlimit"), "", _("Remove all search limits."), command_limit),
        ]
        return {cmd.command: cmd.handler for cmd in cls.commands}

    @classmethod
    def help_text(cls, bot_name="mention-bot"):
        return "{}\n\n{}".format(
            _(
                "This plugin allows users to store and query the tag-set of other users."
            ),
            "\n".join(
                f"- @{bot_name} {cmd.command}: {cmd.syntax}  -> {cmd.help}"
                for cmd in cls.commands
            ),
        )

    def initialize(self, bot_handler: Any) -> None:
        self.storage = ""
        self.config_info = bot_handler.get_config_info("tagger-bot")
        for key, val in self.config_info.items():
            if key == "language":
                lang = gettext.translation(
                    domain="taggerbot", localedir="locale", languages=[val]
                )
                lang.install()
                global _
                _ = lang.gettext
            elif key == "storage" and val.endswith(".json"):
                self.storage = val
        logger.debug(self.config_info)
        self.commands = self.build_commands()

    def usage(self) -> str:
        return self.help_text()

    def handle_message(self, message: Dict[str, str], bot_handler: Any) -> None:
        quoted_name = bot_handler.identity().mention
        original_content = message["content"].strip()

        if self.storage.endswith(".json"):
            storage = JsonFileStorage(self.storage)
        else:
            storage = ZulipStorage(bot_handler)

        if ":" not in original_content:
            command, *params = original_content.strip().split(" ", 1)
        else:
            command, *params = original_content.strip().split(":", 1)
        if "sender_full_name" in message:
            sender = message["sender_full_name"]
        else:
            sender = message["sender_email"]
        try:
            handler = self.commands[command]
            response = handler(sender, command, params, storage, bot_handler)
            if response is not None:
                bot_handler.send_reply(message, response)
        except MissingParameterError:
            bot_handler.send_reply(
                message, _("Sorry, I didn't understand you, a parameter is missing")
            )
        except KeyError as err:
            bot_handler.send_reply(
                message, _("Sorry, '{}' is not a command I understand.").format(err)
            )
        except Exception as err:
            logger.exception(err)


handler_class = TaggerBotHandler
