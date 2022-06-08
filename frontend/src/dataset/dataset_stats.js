import React, {useEffect, useState} from 'react';
import {Header, Loader, Table} from "semantic-ui-react";
import API from "../api";
import util from "../util";

export default function DatasetStats({entity, dataset}) {
    const [stats, setStats] = useState(null)
    useEffect(() => {
        API.getStats(entity.name, dataset.tag).then(result => {
            setStats(result)
        })
    }, [entity, dataset])
    if (stats === null) {
        return <Loader active/>
    } else {
        return (
            <Table>
                <Table.Body>
                    <Table.Row>
                        <Table.Cell><Header sub>Created on</Header></Table.Cell>
                        <Table.Cell>{util.formatTime(dataset.created_at)}</Table.Cell>
                    </Table.Row>
                    <Table.Row>
                        <Table.Cell><Header sub>Last updated</Header></Table.Cell>
                        <Table.Cell>{util.formatTime(dataset.updated_at)}</Table.Cell>
                    </Table.Row>
                    <Table.Row>
                        <Table.Cell><Header sub>Segments</Header></Table.Cell>
                        <Table.Cell>{stats.num_segments}</Table.Cell>
                    </Table.Row>
                    <Table.Row>
                        <Table.Cell><Header sub>Total size</Header></Table.Cell>
                        <Table.Cell>{util.formatBytes(stats.total_size)}</Table.Cell>
                    </Table.Row>
                </Table.Body>
            </Table>
        )
    }
}
