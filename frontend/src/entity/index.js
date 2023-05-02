import React, {useContext, useEffect, useState} from 'react';
import {Header, Loader} from "semantic-ui-react";
import {useParams} from "react-router-dom";
import {Outlet} from "react-router";
import API from "../api";
import {UserContext} from "../auth";

import './entity.css'

export default function EntityProvider() {
    const {entityName} = useParams()
    const user = useContext(UserContext)
    const [entity, setEntity] = useState(null)
    const [failed, setFailed] = useState(null)
    const [owner, setOwner] = useState(null)

    useEffect(async () => {
        setEntity(null)
        setFailed(false)
        setOwner(null)

        try {
            const entity = await API.getEntity(entityName)
            setEntity(entity)
        } catch (ex) {
            setFailed(ex)
        }
        try {
            const {owner} = await API.canManageEntity(entityName)
            setOwner(owner)
        } catch (ex) {
            setFailed(ex)
        }
    }, [user, entityName])

    if (failed) return <Header>
        Unable to access this page
        <Header.Subheader>Are you authorized to be here?</Header.Subheader>
    </Header>
    if (entity === null || entity.name !== entityName || owner === null) return <Loader active/>

    return <Outlet context={{entity: entity, owner: owner}}/>
}
