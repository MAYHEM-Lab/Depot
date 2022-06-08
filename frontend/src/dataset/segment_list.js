import React, {useEffect, useState} from 'react';
import {useNavigate, useOutletContext} from "react-router-dom";

import API from '../api'
import util from '../util.js'
import {Loader, Table} from "semantic-ui-react";
import SegmentState from "./segment_state";

export default function SegmentList() {
    const {entity, dataset} = useOutletContext();
    const navigate = useNavigate();
    const [segments, setSegments] = useState(null)
    useEffect(() => {
        API.getSegments(entity.name, dataset.tag).then(segments => setSegments(segments));
    }, [entity, dataset])

    if (!segments) {
        return <Loader active/>
    } else {
        return <Table selectable textAlign='center'>
            <Table.Header>
                <Table.Row>
                    <Table.HeaderCell>Version</Table.HeaderCell>
                    <Table.HeaderCell>Created</Table.HeaderCell>
                    <Table.HeaderCell>Updated</Table.HeaderCell>
                    <Table.HeaderCell>State</Table.HeaderCell>
                </Table.Row>
            </Table.Header>
            <Table.Body>
                {segments.map(segment =>
                    <Table.Row
                        className='dataset-segment-listing'
                        key={segment.version}
                        onClick={() => navigate(segment.version.toString())}
                    >
                        <Table.Cell>{segment.version}</Table.Cell>
                        <Table.Cell>{util.formatTime(segment.created_at)}</Table.Cell>
                        <Table.Cell>{util.formatTime(segment.updated_at)}</Table.Cell>
                        <Table.Cell><SegmentState segmentState={segment.state}/></Table.Cell>
                    </Table.Row>
                )}
            </Table.Body>
        </Table>
    }
}
