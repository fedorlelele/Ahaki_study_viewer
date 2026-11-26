// Generate a Windows update feed on the release Mac using the existing ASV Sparkle signing key.
import {readFile, writeFile, mkdtemp, rm} from 'node:fs/promises';
import {createReadStream} from 'node:fs';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {filename, readFeed} from '../main/updates.mjs';

const [directory,tool] = process.argv.slice(2);
if (!directory || !tool) throw new Error('node scripts/sign-update.mjs <installer-directory> <Sparkle-sign_update>');
const {version} = JSON.parse(await readFile(new URL('../package.json',import.meta.url)));
const installers={};
for (const arch of ['x64','arm64']) {
  const sha=createHash('sha256'); let bytes=0;
  for await (const chunk of createReadStream(join(directory,filename(version,arch)))) {bytes+=chunk.length; sha.update(chunk);}
  installers[arch]={bytes,sha256:sha.digest('hex')};
}
const payload=Buffer.from(JSON.stringify({product:'ASV-Windows',version,installers}));
const temp=await mkdtemp(join(tmpdir(),'asv-win-feed-'));
try {
  const path=join(temp,'release.bin'); await writeFile(path,payload);
  const signature=execFileSync(resolve(tool),['--account','ASV-Mac-Updates','-p',path],{encoding:'utf8'}).trim();
  const feed=JSON.stringify({payload:payload.toString('base64'),signature},null,2);
  for(const arch of Object.keys(installers)) readFeed(feed,arch);
  await writeFile(join(directory,'windows-update.json'),feed+'\n');
} finally {await rm(temp,{recursive:true,force:true});}
