import {Loader, Segment, Table} from "semantic-ui-react";
import util from "../util";
import React, {useContext, useEffect, useState} from "react";
import API from "../api";

import './organization.css'
import {Link} from "react-router-dom";
import {EventContext} from "../common/bus";

export default function ListOrganizations({entity}) {
    const [orgs, setOrgs] = useState(null)
    const [trigger, setTrigger] = useState(0)

    const eventBus = useContext(EventContext)

    const invalidate = () => setTrigger(t => t + 1)

    useEffect(() => {
        eventBus.on('reload-orgs', invalidate)
        return () => eventBus.remove('reload-orgs', invalidate)
    })

    useEffect(async () => {
        const {entities} = await API.getAuthorizedEntities()
        const orgIds = entities.filter((e) => e.type === 'Organization')
        const orgs = await Promise.all(orgIds.map((e) => API.getEntity(e.name)))
        setOrgs(orgs)
    }, [entity, trigger])
    return <OrganizationList orgs={orgs}/>
}

function OrganizationList({orgs}) {
    return <Segment basic>
        <Loader active={!orgs}/>
        <Table celled singleLine>
            <Table.Header>
                <Table.Row>
                    <Table.HeaderCell>Organization</Table.HeaderCell>
                    <Table.HeaderCell>Creation Date</Table.HeaderCell>
                </Table.Row>
            </Table.Header>
            <Table.Body>
                {orgs ? orgs.map(org =>
                    <Table.Row key={org.name}>
                        <Table.Cell>
                            <Link to={{pathname: `/${org.name}`}}>
                                <code>{org.name}</code>
                            </Link>
                        </Table.Cell>
                        <Table.Cell>{util.formatTime(org.created_at)}</Table.Cell>
                    </Table.Row>
                ) : null}
            </Table.Body>
        </Table>
    </Segment>
}
