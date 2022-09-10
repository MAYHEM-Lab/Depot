import React, {useContext, useState} from "react";
import {UserContext} from "./auth";
import {useSearchParams} from "react-router-dom";
import {Button, Header} from "semantic-ui-react";
import ListDatasets from "./dataset/list";
import ListNotebooks from "./notebook/list";
import ListOrganizations from "./orgs/list";
import OrganizationCreator from "./orgs/creator";
import API from "./api";
import DatasetUploader from "./dataset/uploader";
import {EventContext} from "./common/bus";
import ListClusters from "./cluster/list";
import QuotaUsage from "./quota";

export default function Home() {
    const user = useContext(UserContext)
    const [creatingOrg, setCreatingOrg] = useState(false)
    const [creatingDataset, setCreatingDataset] = useState(false)
    const [searchParams] = useSearchParams()
    const eventBus = useContext(EventContext)

    const view = searchParams.get('view')
    if (!user) return <Header>Log in to continue</Header>
    if (view === 'datasets') {
        return <>
            <Header>
                My Datasets
                <Button size='small' positive floated='right' onClick={() => setCreatingDataset(true)}>Upload dataset</Button>
            </Header>
            <DatasetUploader
                open={creatingDataset}
                onClose={() => setCreatingDataset(false)}
                onCreate={async (owner, tag, description, datatype, visibility, files) => {
                    await API.createUnmanagedDataset(owner, tag, description, datatype, visibility)
                    await API.createUnmanagedSegment(owner, tag, files)
                    setCreatingDataset(false)
                    eventBus.dispatch('reload-datasets', null)
                }}
            />
            <ListDatasets entity={user}/>
        </>
    } else if (view === 'notebooks') {
        return <>
            <Header>My Notebooks</Header>
            <ListNotebooks entity={user}/>
        </>
    } else if (view === 'organizations') {
        return <>
            <Header>
                My Organizations
                <Button size='small' primary floated='right' onClick={() => setCreatingOrg(true)}>New organization</Button>
            </Header>
            <OrganizationCreator
                open={creatingOrg}
                onClose={() => setCreatingOrg(false)}
                onCreate={async (tag) => {
                    await API.createOrganization(tag)
                    setCreatingOrg(false)
                    eventBus.dispatch('reload-orgs', null)
                }}
            />
            <ListOrganizations/>
        </>
    } else if (view === 'resources') {
        return <>
            <Header>Quota Usage</Header>
            <QuotaUsage entity={user}/>

            <Header>My Clusters</Header>
            <ListClusters entity={user}/>
        </>
    } else {
        return <div>Main page</div>
    }
}
