import React, {useContext, useEffect, useState} from 'react';
import {Container, Divider, Header, Loader, Tab} from "semantic-ui-react";
import {Link, useLocation, useParams} from "react-router-dom";
import {Outlet, useOutletContext} from "react-router";
import API from "../api";

import './dataset.css'
import DatasetIcon from "./dataset_icon";
import VisibilityInfo from "./visbility";
import Error from "../common/error";
import {UserContext} from "../auth";

export default function DatasetHeader() {
    const {entity} = useOutletContext();
    const user = useContext(UserContext)
    const {datasetTag} = useParams()
    const location = useLocation()
    const [dataset, setDataset] = useState(null)
    const [failed, setFailed] = useState(null)
    const [owner, setOwner] = useState(null)

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

    }, [user, datasetTag])

    if (failed) return <Error/>
    if (dataset === null || dataset.tag !== datasetTag || owner === null) return <Loader active/>
    const panes = [
        {
            menuItem: {
                as: Link,
                icon: 'info',
                content: 'Overview',
                to: `/${entity.name}/${dataset.tag}`,
                key: 'overview'
            }
        },
        {
            menuItem: {
                as: Link,
                id: 'segments',
                icon: 'list',
                content: 'Segments',
                to: `/${entity.name}/${dataset.tag}/segments`,
                key: 'segments'
            }
        },
        {
            menuItem: {
                as: Link,
                id: 'code',
                icon: 'code',
                content: 'Code',
                to: `/${entity.name}/${dataset.tag}/code`,
                key: 'code'
            }
        },
    ]

    if (owner) {
        panes.push({
            menuItem: {
                as: Link,
                id: 'manage',
                icon: 'settings',
                className: 'dataset-manage-tab',
                content: 'Manage',
                to: `/${entity.name}/${dataset.tag}/manage`,
                key: 'manage'
            }
        })
    }

    const activeTab = panes.findIndex((item) =>
        item.menuItem.id === location.pathname.split('/')[3]
    )

    return (
        <Container fluid className='dataset-item'>
            <Header size='huge'>
                <Header.Content>
                    <DatasetIcon datatype={dataset.datatype}/>
                    <Link to={`/${entity.name}`}>{entity.name}</Link>
                    <span className='resource-divider'>/</span>
                    <Link to={`/${entity.name}/${dataset.tag}`}>{dataset.tag}</Link>
                </Header.Content>
                <VisibilityInfo dataset={dataset} entity={entity}/>
            </Header>
            <Tab
                menu={{secondary: true}}
                renderActiveOnly={true}
                activeIndex={activeTab}
                panes={panes}
            />
            <Divider/>
            <Outlet context={{entity: entity, dataset: dataset}}/>
        </Container>
    )
}