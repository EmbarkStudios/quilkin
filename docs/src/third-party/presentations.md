# Third Party Videos and Presentations

## GDC 2023: Open Source Game Development Summit: Agones and Quilkin: OSS Multiplayer Game Server Service Mesh on Kubernetes

<a href="https://www.gdcvault.com/play/1029264/Open-Source-Game-Development-Summit" target="_blank">
<img src="./vault.png" alt="Presentation recording" />
</a>

(Does not require GDCVault access to watch)

Previous talks have looked at two open source projects: Agones, the platform for orchestrating and scaling 
dedicated multiplayer game servers on top of Kubernetes, and Quilkin, an extensible UDP proxy specifically built 
for protecting, observing and authenticating game server communications.

On the surface this seems like a very powerful combination, but manually integrating the two can be a tricky and 
intimidating process. You need to know and understand the Kubernetes API and its client libraries, and then tie it 
into Quilkin's xDS compliant API implementation all through some custom code - which is a large and complex amount 
of work (and a lot of acronyms).

In this talk, Mark Mandel, Developer Advocate at Google Cloud, will look at how anyone can utilize Quilkin's native 
Agones integration to provide a turnkey solution for proxying UDP traffic to game servers providing an extra layer 
of protection and observability when orchestrating game servers on top of Kubernetes.
