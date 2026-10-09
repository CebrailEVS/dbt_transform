#!/usr/bin/env bash
# Hook: PreToolUse (matcher Bash) → garde-fou de branche avant commit / push / PR.
#
# Deux contrôles déterministes :
#   A. commit ou push sur master avec des fichiers NON docs-only
#      → règle du dépôt : branche de feature obligatoire, sauf docs-only.
#   B. la branche courante descend d'une AUTRE branche de feature locale non
#      mergée → sa PR embarquerait les commits d'un autre chantier.
#
# Exit 0 = laisser passer.  Exit 2 = bloquer, stderr remonte à Claude.
# Échappatoire volontaire : EVS_ALLOW_STACKED=1 désactive le contrôle B.
#
# Principe de sûreté : toute anomalie interne au script → exit 0.
# Un garde-fou qui bloque à tort est pire que pas de garde-fou.

set -uo pipefail

INPUT=$(cat)

COMMAND=$(printf '%s' "$INPUT" | python3 -c \
    "import sys,json;d=json.load(sys.stdin);print(d.get('tool_input',{}).get('command',''))" 2>/dev/null) || exit 0
HOOK_CWD=$(printf '%s' "$INPUT" | python3 -c \
    "import sys,json;d=json.load(sys.stdin);print(d.get('cwd',''))" 2>/dev/null) || exit 0

[[ -n "$COMMAND" ]] || exit 0

# --- Ne réagir que sur les commandes qui gravent ou publient -----------------
if ! grep -Eq '(^|[;&|[:space:]])(git([[:space:]]+-C[[:space:]]+[^[:space:]]+)?[[:space:]]+(commit|push)|gh[[:space:]]+pr[[:space:]]+create)' <<<"$COMMAND"; then
    exit 0
fi

# --- Résoudre le dépôt : `git -C <path>` prime, sinon le cwd de la session ---
REPO_HINT=$(grep -oE 'git[[:space:]]+-C[[:space:]]+[^[:space:]]+' <<<"$COMMAND" | head -1 | awk '{print $3}')
[[ -n "${REPO_HINT:-}" ]] || REPO_HINT="${HOOK_CWD:-$PWD}"
[[ -d "$REPO_HINT" ]] || REPO_HINT="${HOOK_CWD:-$PWD}"

REPO=$(git -C "$REPO_HINT" rev-parse --show-toplevel 2>/dev/null) || exit 0
[[ -n "$REPO" ]] || exit 0

BRANCH=$(git -C "$REPO" rev-parse --abbrev-ref HEAD 2>/dev/null) || exit 0
[[ -n "$BRANCH" && "$BRANCH" != "HEAD" ]] || exit 0

# Pas de remote master connu (repo local pur) → rien à comparer.
git -C "$REPO" rev-parse --verify --quiet origin/master >/dev/null 2>&1 || exit 0

# ============================================================================
# Contrôle A — travail non docs-only sur master
# ============================================================================
if [[ "$BRANCH" == "master" || "$BRANCH" == "main" ]]; then
    # Opt-out par depot. Certains travaillent legitimement en direct sur master :
    # mainteneur unique, aucun relecteur, historique lineaire, pas de CD qui
    # deploie sur push (ex. `infra/`). Ils le declarent par un marqueur versionne
    # plutot que de decabler tout le hook — le controle B (branche empilee) reste
    # actif. Le marqueur est un choix explicite, tracable dans git.
    [[ -f "$REPO/.claude/allow-master-commits" ]] && exit 0

    # Périmètre examiné : index + worktree, contre origin/master.
    CHANGED=$(
        {
            git -C "$REPO" diff --cached --name-only 2>/dev/null
            git -C "$REPO" diff --name-only 2>/dev/null
            git -C "$REPO" log --format= --name-only origin/master..HEAD 2>/dev/null
        } | sort -u | sed '/^$/d'
    )

    if [[ -n "$CHANGED" ]]; then
        # Exemptions : la doc, et la couche IA `.claude/`, qui n'entre dans aucun
        # path filter de CI et ne produit rien en prod.
        NON_DOCS=$(grep -vE '(^|/)(README|CONTRIBUTING|CONVENTIONS|CLAUDE|RUNBOOK[^/]*|TERRAFORM_GUIDE)\.md$|^docs/|/docs/|\.md$|^\.claude/|/\.claude/' <<<"$CHANGED" || true)
        if [[ -n "$NON_DOCS" ]]; then
            {
                echo "BLOQUÉ — branche master avec des fichiers non docs-only."
                echo
                echo "Règle du dépôt : toute modification de code passe par une branche de"
                echo "feature créée depuis origin/master à jour. Seuls les changements"
                echo "docs-only (README, docs/, CONTRIBUTING, CONVENTIONS) vont en direct."
                echo
                echo "Fichiers non docs-only en cause :"
                sed 's/^/  - /' <<<"$NON_DOCS"
                echo
                echo "Correction :"
                echo "  git -C $REPO fetch origin master"
                echo "  git -C $REPO checkout -b feature/<nom> origin/master"
            } >&2
            exit 2
        fi
    fi
    exit 0
fi

# ============================================================================
# Contrôle B — la branche descend d'une autre branche de feature locale
# ============================================================================
[[ "${EVS_ALLOW_STACKED:-0}" == "1" ]] && exit 0

POLLUANTES=""
while IFS= read -r b; do
    [[ -n "$b" && "$b" != "$BRANCH" && "$b" != "master" && "$b" != "main" ]] || continue
    # b est-il dans l'ascendance de HEAD ?
    git -C "$REPO" merge-base --is-ancestor "$b" HEAD 2>/dev/null || continue
    # ... et ses commits sont-ils absents d'origin/master (donc non mergés) ?
    if ! git -C "$REPO" merge-base --is-ancestor "$b" origin/master 2>/dev/null; then
        N=$(git -C "$REPO" rev-list --count "origin/master..$b" 2>/dev/null || echo "?")
        POLLUANTES+="  - $b ($N commit(s) hors origin/master)"$'\n'
    fi
done < <(git -C "$REPO" for-each-ref --format='%(refname:short)' refs/heads/ 2>/dev/null)

if [[ -n "$POLLUANTES" ]]; then
    {
        echo "BLOQUÉ — la branche '$BRANCH' descend d'une autre branche de feature non mergée."
        echo
        echo "Sa PR embarquerait les commits d'un autre chantier."
        echo
        echo "Branche(s) présente(s) dans l'ascendance de HEAD :"
        printf '%s' "$POLLUANTES"
        echo "Commits qui partiraient réellement :"
        git -C "$REPO" log --oneline origin/master..HEAD 2>/dev/null | sed 's/^/  /'
        echo
        echo "Correction non destructive (l'autre branche reste intacte) :"
        echo "  git -C $REPO fetch origin master"
        echo "  git -C $REPO checkout -b <branche>-clean origin/master"
        echo "  git -C $REPO cherry-pick <les seuls commits du chantier en cours>"
        echo
        echo "Empilement volontaire ? Relancer avec EVS_ALLOW_STACKED=1."
    } >&2
    exit 2
fi

exit 0
