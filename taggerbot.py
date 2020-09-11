import collections
import contextlib
import logging
import textwrap

from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


class InvalidCommandError(Exception):
    pass


class MissingParameterError(Exception):
    pass


class TagMapping:
    STORAGE_KEY = "mapping"

    def __init__(self):
        self.tags = collections.defaultdict(set)
        self.users = collections.defaultdict(set)
        self.dirty = False

    def dump(self):
        return {t: list(us) for t, us in self.tags.items() if len(us) > 0}

    def load(self, storage):
        d = storage.get(self.STORAGE_KEY) if storage.contains(self.STORAGE_KEY) else {}
        self.tags.clear()
        self.users.clear()
        self.dirty = False
        for tag, users in d.items():
            self.tags[tag].update(users)
            for user in users:
                self.users[user].add(tag)
        return self

    def store(self, storage):
        if self.dirty:
            storage.put(self.STORAGE_KEY, self.dump())
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
            self.tags[tag].add(user)
            self.users[user].add(tag)

    def remove(self, user, *tags):
        self.dirty = True
        for tag in tags:
            self.tags[tag].discard(user)
            self.users[user].discard(tag)

    def find(self, tag=None, user=None):
        # assert tag is None ^ user is None
        if tag is not None:
            return self.tags[tag]
        if user is not None:
            return self.users[user]
        raise KeyError()


def read_parameters(params):
    if len(params) != 1:
        raise MissingParameterError()
    return set(map(str.strip, params[0].split(",")))


CommandContext = collections.namedtuple(
    "CommandContext", "handler parser", defaults=[lambda *e: None, lambda *e: []]
)


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

    strings = {
        "TAG_LIST": "Hi @**{}**, you are currently tagged with: {}",
        "TAG_SEARCH": "Hi @**{}**, here's a list of everybody tagged with: {}\n\n{}",
        "TAG_JOIN_AND": " and ",
        "TAG_LIMIT": "Tag search is currently limited to: {}",
        "TAG_UNLIMIT": "Tag search is currently unlimited",
        "ERR_PARAM": "Sorry, I didn't understand you, a parameter is missing",
        "ERR_COMMAND": "Sorry, '{}' is not a command I understand.",
        "COMMAND_HELP": "help",
        "COMMAND_LIST": "list",
        "COMMAND_ADD": "add",
        "COMMAND_REMOVE": "remove",
        "COMMAND_SEARCH": "search",
        "COMMAND_LIMIT": "limit",
        "COMMAND_UNLIMIT": "unlimit",
        "HELP_TEXT": """This plugin allows users to store and query the tag-set of other users.""",
        "HELP_HELP": "To show all commands the bot supports.",
        "HELP_LIST": "Show all tags currently applied to the user",
        "HELP_ADD": "To add personal tag(s).",
        "HELP_REMOVE": "To remove personal tag(s).",
        "HELP_SEARCH": "To search for somebody with <tag>.",
        "HELP_LIMIT": "Limit search to this group of users.",
        "HELP_UNLIMIT": "Remove all search limits.",
        "SYNTAX_HELP": "",
        "SYNTAX_LIST": "",
        "SYNTAX_ADD": "<tag>, <tag> ...",
        "SYNTAX_REMOVE": "<tag>, <tag>, ...",
        "SYNTAX_SEARCH": "<tag>, <tag>, ...",
        "SYNTAX_LIMIT": "<user>, <user>, ...",
        "SYNTAX_UNLIMIT": "",
    }

    META = {
        "name": "Tagger",
        "description": "Allows user tag store/lookup",
    }

    def initialize(self, bot_handler: Any) -> None:
        self.config_info = bot_handler.get_config_info("TaggerBot")
        for key, val in self.config_info.items():
            if key.startswith("STRING_"):
                self.strings[key[7:]] = val

    def usage(self) -> str:
        return "{}\n\n{}".format(
            self.strings["HELP_TEXT"],
            "\n".join(
                "- @mention-bot {}: {}  -> {}".format(
                    self.strings["COMMAND_{}".format(cmd)],
                    self.strings["SYNTAX_{}".format(cmd)],
                    self.strings["HELP_{}".format(cmd)],
                )
                for cmd in [
                    "HELP",
                    "LIST",
                    "ADD",
                    "REMOVE",
                    "SEARCH",
                    "LIMIT",
                    "UNLIMIT",
                ]
            ),
        )

    def handle_message(self, message: Dict[str, str], bot_handler: Any) -> None:
        quoted_name = bot_handler.identity().mention
        original_content = message["content"].strip()
        if ":" not in original_content:
            command, *params = original_content.strip().split(" ", 1)
        else:
            command, *params = original_content.strip().split(":", 1)
        if "sender_full_name" in message:
            sender = message["sender_full_name"]
        else:
            sender = message["sender_email"]
        try:
            if command in [self.strings["COMMAND_HELP"]]:
                bot_handler.send_reply(message, self.usage())
            elif command in [
                self.strings["COMMAND_LIST"],
                self.strings["COMMAND_ADD"],
                self.strings["COMMAND_REMOVE"],
            ]:
                cmd_funcs = {
                    self.strings["COMMAND_ADD"]: CommandContext(
                        TagMapping.add, read_parameters
                    ),
                    self.strings["COMMAND_REMOVE"]: CommandContext(
                        TagMapping.remove, read_parameters
                    ),
                }
                ctx = cmd_funcs.get(command, CommandContext())
                all_tags = ctx.parser(params)
                with TagMapping().use(bot_handler.storage) as tags:
                    ctx.handler(tags, sender, *all_tags)
                    bot_handler.send_reply(
                        message,
                        self.strings["TAG_LIST"].format(
                            sender, ", ".join(tags.find(user=sender)),
                        ),
                    )
            elif command in [self.strings["COMMAND_SEARCH"]]:
                all_tags = read_parameters(params)
                with TagMapping().use(bot_handler.storage) as tags:
                    results = [tags.find(tag=tag) for tag in all_tags]
                    limit = set(
                        bot_handler.storage.get("limit")
                        if bot_handler.storage.contains("limit")
                        else []
                    )
                    if len(limit) > 0:
                        intersection = limit.intersection(*results)
                    else:
                        intersection = results[0].intersection(*results[:1])
                    bot_handler.send_reply(
                        message,
                        self.strings["TAG_SEARCH"].format(
                            sender,
                            self.strings["TAG_JOIN_AND"].join(all_tags),
                            "\n".join(
                                "- @**{}**".format(user) for user in intersection
                            ),
                        ),
                    )
            elif command in [self.strings["COMMAND_LIMIT"]]:
                users = read_parameters(params)
                limit = set(
                    bot_handler.storage.get("limit")
                    if bot_handler.storage.contains("limit")
                    else []
                )
                limit = limit.union(users)
                if len(limit) > 0:
                    bot_handler.send_reply(
                        message, self.strings["TAG_LIMIT"].format(", ".join(limit)),
                    )
                else:
                    bot_handler.send_reply(
                        message, self.strings["TAG_UNLIMIT"],
                    )
                bot_handler.storage.put("limit", list(limit))
            elif command in [self.strings["COMMAND_UNLIMIT"]]:
                bot_handler.storage.set("limit", [])
                bot_handler.send_reply(
                    message, self.strings["TAG_UNLIMIT"],
                )
            else:
                raise InvalidCommandError(command)
        except MissingParameterError:
            bot_handler.send_reply(message, self.strings["ERR_PARAM"])
        except InvalidCommandError as err:
            bot_handler.send_reply(message, self.strings["ERR_COMMAND"].format(err))
        except Exception as err:
            logger.exception(err)


handler_class = TaggerBotHandler
