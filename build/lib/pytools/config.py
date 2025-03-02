from yaml import dump as ym_dump, safe_load
from json import dump as js_dump, load


class Node:
    def __init__(self, dt: dict):
        """
        Initialisation of Node as a self-nested Node class
        :param dt: dictionary with parameters which are going to be transformed to Node class element
        """
        if not isinstance(dt, dict):
            raise TypeError("dict type expected.")

        for key, val in dt.items():
            if isinstance(val, dict):
                setattr(self, key, Node(val))
            else:
                setattr(self, key, val)

    def get_node(self, name_nd: str, raise_missing: bool = True) -> 'Node':
        """
        Function allows for searching a subNode of a Node by providing its nd_name
        :param name_nd: name of the node you are looking for
        :param raise_missing: boolean, if True and name_nd branch does not exist, then Error is raised, if False,
               None is returned.
        :return: Node: return branch of supernode named 'name_nd'
        """
        if name_nd in self.__dict__.keys():
            return getattr(self, name_nd)  # if in the scope of Node 'self' is 'name_nd' then returns self
        else:
            for key in self.__dict__.keys():
                if isinstance(getattr(self, key), Node):  # if sub-Node of self named 'key' has type Node then recursively the get_node function is called
                    temp = getattr(self, key).get_node(name_nd, False)
                    if temp is None:
                        pass
                    else:
                        return temp
        if raise_missing:
            raise ValueError(f"'name_nd' does not exists for this Node!")

    def to_dict(self, nd_name: str = None) -> dict:
        """
        Transforms a Node to a dictionary.
        :param nd_name: name of subNode which is going to be dumped to dictionary.
        :return: Node nd_name transformed to dictionary.
        """
        if not (isinstance(nd_name, str) or nd_name is None):
            raise TypeError("nd_name should be a string or None type.")

        dt = dict({})

        if nd_name is not None:
            new_node = self.get_node(nd_name, False)
        else:
            new_node = self

        for key, val in new_node.__dict__.items():
            if isinstance(val, Node):
                dt[key] = val.to_dict()
            else:
                dt[key] = val

        return dt

    def get_parent_node(self, nd_name: str, missing_node: bool = True) -> 'Node':
        """
            Returns parent Node of the subNode with a name 'nd_name'
        :param nd_name: string name of the Node you want to get a parent Node
        :param missing_node: if True and a such branch does not exist, an error is raised, if False,
               then None is returned
        :return: Node - parent node which has a branch 'nd_name'
        """
        if not isinstance(nd_name, str):
            raise TypeError("nd_name should be a string")

        if nd_name in self.__dict__.keys():
            return self
        else:
            for val in self.__dict__.values():
                if isinstance(val, Node):
                    temp = val.get_parent_node(nd_name, False)
                    if temp is not None:
                        return temp
        if missing_node:
            raise ValueError(f"No branch of the node has name {nd_name}!")

    def mod_val(self, nd_name: str, value):
        """
        Modifies value of branch nd_name by value, value must have the same type as nd_name branch and cannot be a Node.
        For modification of Node there should be applied combination of methods 'del_node' and 'glue_nodes'.
        :param nd_name: name of branch-attribute that value should be modified
        :param value: replacing value (can be variant object)
        :return: None
        """
        temp = self.get_node(nd_name)
        if (not isinstance(temp, Node)) and isinstance(value, type(temp)):
            setattr(self.get_parent_node(nd_name, missing_node=True), nd_name, value)
        del temp

    def del_node(self, nd_name: str):
        """
        Deletes a subNode with a branch name 'nd_name'.
        :param nd_name: string with a node_name
        :return: None
        """
        delattr(self.get_parent_node(nd_name, missing_node=True), nd_name)

    def glue_nodes(self, other: 'Node', to_glue_nd: str, other_name: str):
        """
        Glues one Node to another as its branch
        :param other: a Node to glue
        :param to_glue_nd: a name of branch to which 'other' Node is going to be glued
        :param other_name: name of the 'other' Node as a branch
        :return: None
        """
        if not isinstance(other_name, str):
            raise TypeError("'other_name' should be a string!")

        if not isinstance(self.get_node(to_glue_nd), Node):
            raise TypeError(f"'to_glue_nd' should be a subNode of this Node!")

        if not isinstance(other, Node):
            raise TypeError("'other' should be a Node!")

        setattr(self.get_node(to_glue_nd), other_name, other)

    def to_json(self, path: str):
        """
        Saves Node to json file.
        :param path: a string containing a path where Node is going to be saved as json file
        :return: None
        """
        with open(path, 'w') as file:
            js_dump(self.to_dict(), file)

    def to_yaml(self, path: str):
        """
        Saves Node to yaml file.
        :param path: a string containing a path where Node is going to be saved as yaml file
        :return: None
        """
        with open(path, 'w') as file:
            ym_dump(self.to_dict(), file, default_flow_style=False)


class Config(Node):
    def __init__(self, path):
        if path[-4:].lower() == 'json':
            super(Config, self).__init__(load(open(path)))
        elif path[-4:] == 'yaml' or path[-3:] == 'yml':
            with open(path, 'r') as stream:
                super(Config, self).__init__(safe_load(stream))


def to_dictionary(node: Node, nd_name: str = None) -> dict:
    """
    Transforms a Node to a dictionary.
    :param node: Node to be transformed to a dictionary
    :param nd_name: name of subNode which is going to be dumped to dictionary.
    :return: Node nd_name transformed to dictionary.
    """
    if not (isinstance(nd_name, str) or nd_name is None):
        raise TypeError("nd_name should be a string or None type.")

    if not (isinstance(node, Node)):
        raise TypeError("node should be a Node object.")

    dt = dict({})

    if nd_name is not None:
        new_node = node.get_node(nd_name)
    else:
        new_node = node

    for key, val in new_node.__dict__.items():
        if isinstance(val, Node):
            dt[key] = to_dictionary(val)
        else:
            dt[key] = val

    return dt


if __name__ == "__main__":
    pass
