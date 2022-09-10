import {Button, Loader, Segment, Table} from "semantic-ui-react";
import util from "../util";
import React, {useContext, useEffect, useState} from "react";
import API from "../api";
import {EventContext} from "../common/bus";
import {Link} from "react-router-dom";
import {ClusterSelector} from "./notebook_frame";
import {UserContext} from "../auth";

export default function ListNotebooks({entity}) {
    const [notebooks, setNotebooks] = useState(null)
    useEffect(() => {
        API.getNotebooks(entity.name).then(setNotebooks)
    }, [entity])
    return <NotebookTable entity={entity} notebooks={notebooks}/>
}

function NotebookTable({entity, notebooks}) {
    const eventBus = useContext(EventContext)
    const user = useContext(UserContext)
    return <Segment basic>
        <Loader active={!notebooks}/>
        <Table celled singleLine>
            <Table.Header>
                <Table.Row textAlign='center'>
                    <Table.HeaderCell>Notebook</Table.HeaderCell>
                    <Table.HeaderCell>Creation Date</Table.HeaderCell>
                    <Table.HeaderCell>Last Modified</Table.HeaderCell>
                    <Table.HeaderCell/>
                </Table.Row>
            </Table.Header>
            <Table.Body>
                {notebooks ? notebooks.map(notebook =>
                    <Table.Row textAlign='center' key={notebook.tag}>
                        <Table.Cell>
                            <code>
                                {notebook.tag}
                            </code>
                        </Table.Cell>
                        <Table.Cell>{util.formatTime(notebook.created_at)}</Table.Cell>
                        <Table.Cell>{util.formatTime(notebook.updated_at)}</Table.Cell>
                        <Table.Cell>
                            <Button size='tiny' as={Link} to={`${entity.name}/notebooks/${notebook.tag}`}
                            >View</Button>
                            <ClusterSelector
                                user={user}
                                trigger={<Button
                                    size='tiny'
                                    primary
                                >Open</Button>}
                                onSelect={(e, t) => eventBus.dispatch('notebook', {
                                    action: 'open',
                                    notebookId: notebook.tag,
                                    entityName: e,
                                    clusterName: t
                                })}
                            />
                        </Table.Cell>
                    </Table.Row>
                ) : null}
            </Table.Body>
        </Table>
    </Segment>
}