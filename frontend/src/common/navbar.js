import React, {useContext} from "react";
import {UserContext} from "../auth";
import {Icon, Menu} from "semantic-ui-react";
import {Link} from "react-router-dom";

export default function NavBar() {
    const user = useContext(UserContext)
    return (
        <Menu>
            <Menu.Item icon as={Link} to='/'>
                <Icon name='home'/>
            </Menu.Item>

            <Menu.Item
                as={Link}
                to={user ? `/?view=datasets` : '/'}
                disabled={!user}
                name='datasets'
            >
                Datasets
            </Menu.Item>

            <Menu.Item
                as={Link}
                to={user ? `/?view=organizations` : '/'}
                disabled={!user}
                name='organizations'
            >
                Organizations
            </Menu.Item>

            <Menu.Item
                as={Link}
                to={user ? `/?view=notebooks` : '/'}
                disabled={!user}
                name='notebooks'
            >
                Saved Notebooks
            </Menu.Item>

            <Menu.Item
                as={Link}
                to={user ? `/?view=clusters` : '/'}
                disabled={!user}
                name='clusters'
            >
                Clusters
            </Menu.Item>
        </Menu>
    )
}