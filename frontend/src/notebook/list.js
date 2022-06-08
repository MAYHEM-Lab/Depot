import {Loader, Segment, Table} from "semantic-ui-react";
import util from "../util";
import React, {useEffect, useState} from "react";
import API from "../api";

export default function ListNotebooks({entity}) {
    const [notebooks, setNotebooks] = useState(null)
    useEffect(() => {
        API.getNotebooks(entity.name).then(setNotebooks)
    }, [entity])
    return <NotebookTable entity={entity} notebooks={notebooks}/>
}

function NotebookTable({notebooks}) {
    return <Segment basic>
        <Loader active={!notebooks}/>
        <Table celled singleLine>
            <Table.Header>
                <Table.Row>
                    <Table.HeaderCell>Notebook</Table.HeaderCell>
                    <Table.HeaderCell>Creation Date</Table.HeaderCell>
                    <Table.HeaderCell>Last Modified</Table.HeaderCell>
                </Table.Row>
            </Table.Header>
            <Table.Body>
                {notebooks ? notebooks.map(notebook =>
                    <Table.Row key={notebook.tag}>
                        <Table.Cell>
                            {notebook.tag}
                        </Table.Cell>
                        <Table.Cell>{util.formatTime(notebook.created_at)}</Table.Cell>
                        <Table.Cell>{util.formatTime(notebook.updated_at)}</Table.Cell>
                    </Table.Row>
                ) : null}
            </Table.Body>
        </Table>
    </Segment>
}