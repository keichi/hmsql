#!/usr/bin/env python3

import sys
from datetime import datetime, timezone, timedelta

import astroid
from pony import orm
from pygit2 import Repository, GIT_SORT_REVERSE


db = orm.Database()


class Commit(db.Entity):
    sha1 = orm.Required(bytes)
    commit_time = orm.Required(datetime)
    modules = orm.Set("Module")


class Module(db.Entity):
    name = orm.Required(str)
    classes = orm.Set("Class")
    functions = orm.Set("Function")
    attributes = orm.Set("Attribute")
    commit = orm.Required(Commit)


class Class(db.Entity):
    name = orm.Required(str)
    functions = orm.Set("Function")
    attributes = orm.Set("Attribute")
    module = orm.Required(Module)
    start_lineno = orm.Required(int)
    end_lineno = orm.Required(int)


class Function(db.Entity):
    name = orm.Required(str)
    module = orm.Optional(Module)
    klass = orm.Optional(Class)
    start_lineno = orm.Required(int)
    end_lineno = orm.Required(int)


class Attribute(db.Entity):
    name = orm.Required(str)
    module = orm.Optional(Module)
    klass = orm.Optional(Class)


#  @profile
@orm.db_session
def analyze_file(path, blob, _commit):
    print(f"Analyzing file {'/'.join(path)}")

    file = blob.data.decode("utf-8")

    try:
        root = astroid.parse(file)
    except astroid.exceptions.AstroidSyntaxError:
        return

    module = Module(name="/".join(path), commit=_commit)

    for node in root.values():
        if isinstance(node, astroid.nodes.ClassDef):
            klass = Class(name=node.name, module=module,
                          start_lineno=node.lineno,
                          end_lineno=node.tolineno)

            for attr_name in node.instance_attrs:
                Attribute(name=attr_name, klass=klass)

            for child in node.get_children():
                if isinstance(child, astroid.nodes.FunctionDef):
                    Function(name=child.name, klass=klass,
                             start_lineno=node.lineno,
                             end_lineno=node.tolineno)

        elif isinstance(node, astroid.nodes.FunctionDef):
            Function(name=node.name, module=module,
                     start_lineno=node.lineno,
                     end_lineno=node.tolineno)

        elif isinstance(node, astroid.nodes.AssignName):
            Attribute(name=node.name, module=module)


@orm.db_session
def analyze_commit(commit):
    print(f"Analyzing commit {commit.id}")

    commit_tz = timezone(offset=timedelta(minutes=commit.commit_time_offset))
    commit_time = datetime.fromtimestamp(commit.commit_time, tz=commit_tz)
    _commit = Commit(sha1=commit.id.raw,
                     commit_time=commit_time)

    stack = [([], commit.tree)]

    while stack:
        path, parent = stack.pop()
        for child in parent:
            if child.type_str == "tree":
                stack.append((path + [child.name], child))
            elif child.type_str == "blob":
                if not child.name.endswith(".py"):
                    continue
                analyze_file(path + [child.name], child, _commit)

    astroid.astroid_manager.MANAGER.clear_cache()

    from astroid.cache import clear_caches

    clear_caches()


def main():
    repo = Repository(sys.argv[1])

    #  db.bind(provider="sqlite", filename=":memory:")
    db.bind(provider="sqlite", filename="db.sqlite3", create_db=True)
    db.generate_mapping(create_tables=True)

    for commit in repo.walk(repo.head.target, GIT_SORT_REVERSE):
        analyze_commit(commit)


if __name__ == "__main__":
    main()
