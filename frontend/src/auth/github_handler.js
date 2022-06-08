import {useEffect} from "react";
import {useSearchParams} from "react-router-dom";

export default function GithubHandler() {
    const [searchParams] = useSearchParams()
    const code = searchParams.get('code')

    useEffect(() => {
        if (window.opener) {
            window.opener.postMessage({auth: {code: code}})
        }
        window.close()
    })
    return null
}