# Le score de pertinence BM25

Le score de pertinence **BM25** (Best Matching 25) est un algorithme probabiliste utilisé en recherche d'information, notamment dans les moteurs comme Elasticsearch, pour classer les documents selon leur pertinence par rapport à une requête. Il est particulièrement adapté au "text mining" ou à la recherche textuelle, où l'on analyse des corpus de textes pour extraire les plus pertinents. Contrairement à TF-IDF simple, BM25 affine le calcul en tenant compte de saturations et de normalisations. [luigisbox](https://www.luigisbox.fr/glossaire-recherche/bm25/)

## BM25

### Formule de base
La formule BM25 pour un document D et une requête Q est la somme sur chaque terme t de la requête :
$$
\text{score}(D, Q) = \sum_{t \in Q} \text{IDF}(t) \cdot \frac{f(t, D) \cdot (k_1 + 1)}{f(t, D) + k_1 \cdot \left(1 - b + b \cdot \frac{|D|}{\text{avgdl}}\right)}
$$

où :
- $f(t, D)$ : fréquence du terme $t$ dans $D$ (term frequency, TF),
- $|D|$ : longueur du document $D$,
- $\text{avgdl}$ : longueur moyenne des documents dans la collection,
- $\text{IDF}(t) = \log \left( \frac{N - n(t) + 0.5}{n(t) + 0.5} \right)$ : inverse document frequency (fréquence inverse du document), avec $N$ le nombre total de documents et $n(t)$ le nombre de documents contenant $t$,
- $k_1$ (typiquement 1.2-2.0) : contrôle la saturation de la TF,
- $b$ (typiquement 0.75) : contrôle l'impact de la longueur du document. [dev](https://dev.to/pykpyky/comprendre-et-implementer-lalgo-de-score-bm25-47af)

### Composantes clés
- **TF saturée** : Le numérateur $f(t, D) \cdot (k_1 + 1)$ évite qu'un terme très fréquent domine excessivement, grâce à la division qui sature pour de hautes fréquences.
- **Normalisation par longueur** : Le dénominateur pénalise les documents trop longs (si $b > 0$), favorisant ceux concis mais riches en termes pertinents. [learn.microsoft](https://learn.microsoft.com/fr-fr/azure/search/index-similarity-and-scoring)
- **IDF** : Réduit l'apport des termes trop communs (ex. "le", "de"). [dev](https://dev.to/pykpyky/comprendre-et-implementer-lalgo-de-score-bm25-47af)

### Exemple simple
Pour une requête "chat noir" sur un document de 100 mots (avgdl=80) contenant "chat" 3 fois et "noir" 1 fois, avec IDF("chat")=1.2 et IDF("noir")=0.9, $k_1=1.5$, $b=0.75$ :

- Score $\text("chat") \approx 1.2 \cdot \frac{3 \cdot 2.5}{3 + 1.5 \cdot (1 - 0.75 + 0.75 \cdot 1.25)} \approx 2.1$,
- Score $\text("noir") \approx 0.9 \cdot \frac{1 \cdot 2.5}{1 + 1.5 \cdot 1.25} \approx 1.0$,
- Total $\approx$ 3.1, comparé à un autre document pour le classement. [dev](https://dev.to/pykpyky/comprendre-et-implementer-lalgo-de-score-bm25-47af)

En text mining, BM25 excelle pour des tâches comme la recherche sémantique sparse ou le ranking initial avant des modèles denses. [emschwartz](https://emschwartz.me/understanding-the-bm25-full-text-search-algorithm/)

BM25 et TF-IDF sont deux méthodes de pondération pour la recherche d'information et le text mining, TF-IDF étant plus simple tandis que BM25 est une évolution probabiliste plus sophistiquée. [kmwllc](https://kmwllc.com/index.php/2020/03/20/understanding-tf-idf-and-bm-25/)

## Différence avec TF-IDF

### TF-IDF de base
**TF-IDF** (Term Frequency-Inverse Document Frequency) calcule un score comme $\text{TF-IDF}(t, D) = \text{TF}(t, D) \times \text{IDF}(t)$, où TF est la fréquence brute du terme dans le document et $IDF \approx \log \left( \frac{N}{n(t)} \right)$ (N : total documents, n(t) : documents contenant t). Il favorise les termes fréquents dans un document mais rares globalement, sans normalisation fine. [ai-bites](https://www.ai-bites.net/tf-idf-and-bm25-for-rag-a-complete-guide/)

### Améliorations de BM25
BM25 affine cela avec :
- **Saturation TF** : Remplace TF brut par $\frac{\text{TF} \cdot (k_1 + 1)}{\text{TF} + k_1 \cdot (1 - b + b \cdot \frac{|D|}{\text{avgdl}})}$, limitant l'impact excessif des hautes fréquences ($k1 \approx 1.2-2.0$)  [kmwllc](https://kmwllc.com/index.php/2020/03/20/understanding-tf-idf-and-bm-25/).
- **Normalisation longueur** : Pénalise les documents longs via b ($\approx 0.75$) et avgdl (longueur moyenne). [kmwllc](https://kmwllc.com/index.php/2020/03/20/understanding-tf-idf-and-bm-25/)
- **IDF lissé** : $\log \left( \frac{N - n(t) + 0.5}{n(t) + 0.5} \right)$, évitant divisions par zéro. [elastic](https://www.elastic.co/fr/blog/practical-bm25-part-2-the-bm25-algorithm-and-its-variables)

### Comparaison pratique
| Aspect              | TF-IDF                          | BM25                                   |
|---------------------|---------------------------------|----------------------------------------|
| TF                  | Linéaire (fréquence brute)     | Saturée (rendements décroissants)  [olafuraron](https://olafuraron.is/blog/bm25vstfidf/) |
| Longueur document   | Ignorée                        | Normalisée (favorise concision)  [kmwllc](https://kmwllc.com/index.php/2020/03/20/understanding-tf-idf-and-bm-25/) |
| IDF                 | Basique $\log(N/n(t))$     | Lissée pour robustesse  [elastic](https://www.elastic.co/fr/blog/practical-bm25-part-2-the-bm25-algorithm-and-its-variables)         |
| Cas idéal           | Petits corpus uniformes  [youtube](https://www.youtube.com/watch?v=EpyHl96-Kyo)| Grands corpus variés  [youtube](https://www.youtube.com/watch?v=EpyHl96-Kyo)   |

BM25 surpasse souvent TF-IDF en précision réelle, surtout sur des textes longs ou hétérogènes, comme dans Elasticsearch. [createur2site](https://createur2site.fr/seo/fonctionnement-google/contenu/)
