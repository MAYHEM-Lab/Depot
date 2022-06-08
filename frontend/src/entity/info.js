import React from "react";
import {Header} from "semantic-ui-react";
import {useOutletContext} from "react-router";
import ListDatasets from "../dataset/list";
import {Link} from "react-router-dom";
import EntityIcon from "./icon";

import API from '../api'
import EntitySet from "../common/acl";

export default function EntityInfo() {
    const {entity, owner} = useOutletContext();
    return <>
        <Header size='huge'>
            <Header.Content>
                <EntityIcon type={entity.type}/>
                <Link to={`/${entity.name}`}>{entity.name}</Link>
            </Header.Content>
            <Header.Subheader>{entity.type}</Header.Subheader>
        </Header>
        <Header>Datasets</Header>
        <ListDatasets entity={entity}/>

        {entity.type === 'Organization' ?
            <EntitySet
                owner={owner}
                entity={entity}
                getEntities={() => API.getMembers(entity.name)}
                addEntity={member => API.addMember(entity.name, member)}
                removeEntity={member => API.removeMember(entity.name, member)}
                existsMsg={'Already in organization'}
                addMsg={'Add to organization'}
                addTitle={'Add member'}
                title={'Members'}
                usersOnly={true}
            /> :
            null
        }
    </>
}