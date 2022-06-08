import React from 'react';
import EntitySet from "../common/acl";
import API from "../api";
import {useOutletContext} from "react-router";

export default function DatasetManage() {
    const {entity, dataset} = useOutletContext();
    return <>
        <EntitySet
            owner={entity.id === dataset.owner_id}
            entity={entity}
            disable={entity.name}
            getEntities={() => API.getDatasetCollaborators(entity.name, dataset.tag)}
            addEntity={member => API.addDatasetCollaborator(entity.name, dataset.tag, member)}
            removeEntity={member => API.delDatasetCollaborator(entity.name, dataset.tag, member)}
            existsMsg={'Already has access'}
            addMsg={'Add collaborator'}
            addTitle={'Add collaborator'}
            title={'Collaborators'}
            usersOnly={false}
        />
    </>
}
