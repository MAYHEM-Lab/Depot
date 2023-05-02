import React, {useContext, useEffect, useState} from 'react';
import {Button, Container, Divider, Header, Loader, Modal, Tab} from "semantic-ui-react";
import {Link, useLocation, useParams} from "react-router-dom";
import {Outlet, useOutletContext} from "react-router";
import API from "../api";

import './dataset.css'
import DatasetIcon from "./icon";
import VisibilityInfo from "./visbility";
import Error from "../common/error";
import {UserContext} from "../auth";
import {FileUpload} from "./uploader";

function SegmentUploader({entity, dataset, open, onClose}) {
    const [loading, setLoading] = useState(false)
    const [files, setFiles] = useState([])

    const uploadable = files.length &&
        !files.find(f => f.state !== 'success') &&
        new Set(files.map(f => f.name)).size === files.length

    const create = async () => {
        setLoading(true)
        try {
            await API.createUnmanagedSegment(entity.name, dataset.tag, files.reduce((o, f) => {
                o[f.name] = f.filename
                return o
            }, {}))
        } finally {
            setLoading(false)
            onClose()
        }
    }

    return <Modal
        onUnmount={() => {
            setLoading(false)
            setFiles([])
        }}
        dimmer='inverted'
        centered={false}
        size='large'
        open={open}
        onClose={onClose}
    >
        <Modal.Content className='dataset-upload-menu'>
            <Header>Upload New Segment</Header>
            <div>
                <FileUpload onFiles={setFiles}/>
                <Button disabled={!uploadable} loading={loading} primary onClick={create}>Create</Button>
            </div>
        </Modal.Content>
    </Modal>
}


export default function DatasetHeader() {
    const {entity} = useOutletContext();
    const user = useContext(UserContext)
    const {datasetTag} = useParams()
    const location = useLocation()
    const [dataset, setDataset] = useState(null)
    const [failed, setFailed] = useState(null)
    const [owner, setOwner] = useState(null)
    const [renderVersion, setRenderVersion] = useState(0)
    const [uploading, setUploading] = useState(false)

    const invalidate = () => {
        setRenderVersion(renderVersion + 1)
    }

    useEffect(async () => {
        setDataset(null)
        setFailed(false)
        setOwner(null)
        try {
            const dataset = await API.getDataset(entity.name, datasetTag)
            setDataset(dataset)
        } catch (ex) {
            setFailed(ex)
        }
        try {
            const {owner} = await API.canManageDataset(entity.name, datasetTag)
            setOwner(owner)
        } catch (ex) {
            setFailed(ex)
        }

    }, [user, datasetTag, renderVersion])

    if (failed) return <Error/>
    if (dataset === null || dataset.tag !== datasetTag || owner === null) return <Loader active/>
    const panes = [
        {
            menuItem: {
                as: Link,
                icon: 'info',
                content: 'Overview',
                to: `/${entity.name}/datasets/${dataset.tag}`,
                key: 'overview'
            }
        },
        {
            menuItem: {
                as: Link,
                id: 'segments',
                icon: 'list',
                content: 'Segments',
                to: `/${entity.name}/datasets/${dataset.tag}/segments`,
                key: 'segments'
            }
        },
    ]

    if (dataset.origin === 'Managed' || dataset.origin === 'Streaming') {
        panes.push({
            menuItem: {
                as: Link,
                id: 'code',
                icon: 'code',
                content: 'Code',
                to: `/${entity.name}/datasets/${dataset.tag}/code`,
                key: 'code'
            }
        })
    }

    if (owner) {
        panes.push({
            menuItem: {
                as: Link,
                id: 'manage',
                icon: 'settings',
                className: 'dataset-manage-tab',
                content: 'Manage',
                to: `/${entity.name}/datasets/${dataset.tag}/manage`,
                key: 'manage'
            }
        })
    }

    const activeTab = panes.findIndex((item) =>
        item.menuItem.id === location.pathname.split('/')[4]
    )

    return (
        <Container fluid className='dataset-item'>
            <Header size='huge'>
                <Header.Content>
                    <DatasetIcon datatype={dataset.datatype}/>
                    <Link to={`/${entity.name}`}>{entity.name}</Link>
                    <span className='resource-divider'>/</span>
                    <Link to={`/${entity.name}/datasets/${dataset.tag}`}>{dataset.tag}</Link>
                </Header.Content>
                <VisibilityInfo dataset={dataset}/>
                <Header.Subheader>{dataset.origin}</Header.Subheader>
                {(owner && dataset.origin === 'Unmanaged') ?
                    <Button size='small' positive onClick={() => setUploading(true)} floated='right'>Upload new segment</Button> :
                    null
                }
                <SegmentUploader entity={entity} dataset={dataset} open={uploading} onClose={() => {
                    setUploading(false)
                    invalidate()
                }}/>
            </Header>
            <Tab
                menu={{secondary: true}}
                renderActiveOnly={true}
                activeIndex={activeTab}
                panes={panes}
            />
            <Divider/>
            <Outlet context={{entity: entity, dataset: dataset, owner: owner, invalidate: invalidate}}/>
        </Container>
    )
}