const Redis = require('redis');
const client = new Redis.createClient({    
    url: 'redis://redisearch:6379'
});
client.on('error', (err) => {
    if (!useable) return;
    console.error(err);
    useable = false;
});

let useable = true;

const initialize =  async () => {
    if (!useable) return;
    try {
        await client.connect();
        await client.ft.create('idx:works', {
            '$.index': {
                type: Redis.SchemaFieldTypes.NUMERIC,
                SORTABLE: true
            },
            '$.url': {
                type: Redis.SchemaFieldTypes.TEXT,
                AS: 'url'
            }
        }, {
            ON: 'JSON',
            PREFIX: 'work:'
        });
    } catch (e) {
        if (e.message === 'Index already exists') {
            console.log('Index exists already, skipped creation.');
        } else {
            useable = false;
            console.error(e);
        }
    }
};

const setWork = async (work) => {
    if (!useable) return;
    try{
        client.json.set(`work:${work.id}`, '$', work);
    } catch (e) {
        if (e.message === 'idx:works: no such index') {
            initialize();
        } else {
            console.error(e);
        }
    }
}

const getWorks = async (url) => {
    if (!useable) return;
    try{  
        let result = await client.ft.search(
        'idx:works',
        url,
        {
            LIMIT: {
                from: 0,
                size: 100
            }
        }
        );
        return result.documents.map(doc => doc.value);
    } catch (e) {
        if (e.message === 'idx:works: no such index') {
            initialize();
        } else {
            console.error(e);
        }
    }
}

const drop = async () => {
    if (!useable) return;
    client.FLUSHDB('ASYNC');
}

const setHistory = async (body) => {
    if (!useable) return;
    try{
        //取得work:${body.id}的值
        let work = await client.json.get(`work:${body.id}`, '$');
        if(work === null || work === undefined){
            return;
        }
        let newValue = [{
            play_time: body.play_time,
            user_name: body.username,
            track_name: body.track_name
        }];
        if(work.history === null){
            await client.json.set(`work:${body.id}`, `$.history`, newValue);
        }else{
            for(let i = 0; i < work.history.length; i++){
                if(work.history[i].user_name === body.username){
                    work.history.splice(i, 1);
                    break;
                }
            }
            work.history.unshift(newValue[0]);
            await client.json.set(`work:${body.id}`, `$.history`, work.history);
        }
    } catch (e) {
        if (e.message === 'idx:works: no such index') {
            initialize();
        } else {
            console.error(e);
        }
    }
}

initialize();

module.exports = {setWork, getWorks, drop, setHistory, useable};