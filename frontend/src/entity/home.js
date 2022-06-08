import React, {useContext, useState} from "react";
import {UserContext} from "../auth";
import {useSearchParams} from "react-router-dom";
import {Button, Header} from "semantic-ui-react";
import ListDatasets from "../dataset/list";
import ListNotebooks from "../notebook/list";
import ListOrganizations from "../orgs/list";
import OrganizationCreator from "../orgs/creator";
import API from "../api";

export default function Home() {
    const user = useContext(UserContext)
    const [creatingOrg, setCreatingOrg] = useState(false)

    const [searchParams] = useSearchParams()
    const view = searchParams.get('view')
    if (!user) return <Header>Log in to continue</Header>

    if (view === 'datasets') {
        return <>
            <Header>My Datasets</Header>
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
                }}
            />
            <ListOrganizations/>
        </>
    } else if (view === 'clusters') {
        return <>
            <Header>My Clusters</Header>
        </>
    } else {
        return <div>Main page</div>
    }
}
