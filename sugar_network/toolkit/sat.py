# Copyright (C) 2010 Thomas Leonard
# Copyright (C) 2014 Aleksey Lim
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# The design of this solver is very heavily based on the one described in
# the MiniSat paper "An Extensible SAT-solver [extended version 1.2]"
# http://minisat.se/Papers.html
#
# The main differences are:
#
# - We care about which solution we find (not just "satisfiable" or "not").
# - We take care to be deterministic (always select the same versions given
#   the same input). We do not do random restarts, etc.
# - We add an _AtMostOneClause (the paper suggests this in the Excercises, and
#   it's very useful for our purposes).

import logging
from collections import deque

from sugar_network.toolkit import enforce


_logger = logging.getLogger('sat')


def solve(clauses, at_most_one_clauses):
    if not clauses:
        _logger.info('No clauses')
        return None

    max_var = 0
    for clause in clauses:
        max_var = max(max_var, max([abs(i) for i in clause]))
    for clause in at_most_one_clauses.values():
        max_var = max(max_var, max([abs(i) for i in clause]))
    problem = _Problem(max_var)

    for clause in clauses:
        if problem.add_clause(clause) is False:
            _logger.info('The %r clause is unresolved', clause)
            return None
    clauses = {}
    for name, clause in at_most_one_clauses.items():
        clauses[name] = problem.at_most_one(clause)

    def decide():
        for clause in clauses.values():
            if clause.current is not None:
                continue
            v = clause.best_undecided()
            if v is not None:
                return v

    if not problem.run_solver(decide):
        return None

    result = {}
    for name, clause in clauses.items():
        if clause.current is not None:
            result[name] = clause.current
    return result


class _AtMostOneClause(object):

    def __init__(self, problem, varset):
        self.problem = problem
        self.varset = varset

        # The single literal from our set that is True.
        # We store this explicitly because the decider needs to know quickly.
        self.current = None

    def propagate(self, v):
        # Re-add ourselves to the watch list.
        # (we we won't get any more notifications unless we backtrack,
        # in which case we'd need to get back on the list anyway)
        self.problem.watch(v, self)

        # value[v] has just become True
        assert self.problem.value(v) is True

        # If we already propagated successfully when the first
        # one was set then we set all the others to False and
        # anyone trying to set one True will get rejected. And
        # if we didn't propagate yet, current will still be
        # None, even if we now have a conflict (which we'll
        # detect below).
        assert self.current is None

        self.current = v

        # If we later backtrace, call our undo function to unset current
        self.problem.undo[abs(v)].append(self)

        for v_ in self.varset:
            value = self.problem.value(v_)
            if value is True and v_ != v:
                # Due to queuing, we might get called with current = None
                # and two versions already selected.
                _logger.trace('CONFLICT already selected %s', v_)
                return False
            if value is None:
                # Since one of our varset is already true, all unknown ones
                # can be set to False.
                if not self.problem.enqueue(-v_, self):
                    _logger.trace('CONFLICT enqueue failed for %s', v_)
                    return False    # Conflict; abort

        return True

    def undo(self, v):
        _logger.trace('Undo %s', v)
        assert v == self.current
        self.current = None

    # Why is v True?
    # Or, why are we causing a conflict (if v is None)?
    def calc_reason(self, v):
        if v is None:
            # Find two True literals
            trues = []
            for v_ in self.varset:
                if self.problem.value(v_) is True:
                    trues.append(v_)
                    if len(trues) == 2:
                        return trues
        else:
            for v_ in self.varset:
                if v_ != v and self.problem.value(v_) is True:
                    return [v_]
            # Find one True literal
        assert 0    # don't know why!

    def best_undecided(self):
        for v in self.varset:
            if self.problem.value(v) is None:
                _logger.trace('Best undecided %r from %r', v, self.varset)
                return v
        return None

    def __repr__(self):
        return "<at most one: %r>" % self.varset


class _UnionClause(object):

    def __init__(self, problem, varset):
        self.problem = problem
        self.varset = varset

    # Try to infer new facts.
    # We can do this only when all of our literals are False except one,
    # which is undecided. That is,
    #   False... or X or False... = True  =>  X = True
    #
    # To get notified when this happens, we tell the solver to
    # watch two of our undecided literals. Watching two undecided
    # literals is sufficient. When one changes we check the state
    # again. If we still have two or more undecided then we switch
    # to watching them, otherwise we propagate.
    #
    # Returns False on conflict.
    def propagate(self, v):
        # value[get(v)] has just become False

        # For simplicity, only handle the case where self.varset[1]
        # is the one that just got set to False, so that:
        # - value[varset[0]] = None | True
        # - value[varset[1]] = False
        # If it's the other way around, just swap them before we start.
        if self.varset[0] == -v:
            self.varset[0], self.varset[1] = self.varset[1], self.varset[0]

        if self.problem.value(self.varset[0]) is True:
            # We're already satisfied. Do nothing.
            self.problem.watch(v, self)
            return True

        assert self.problem.value(self.varset[1]) is False

        # Find a new literal to watch now that varset[1] is resolved,
        # swap it with varset[1], and start watching it.
        for i in range(2, len(self.varset)):
            value = self.problem.value(self.varset[i])
            if value is not False:
                # Could be None or True. If it's True then we've already done
                # our job, so this means we don't get notified
                # unless we backtrack, which is fine.
                self.varset[1], self.varset[i] = self.varset[i], self.varset[1]
                self.problem.watch(-self.varset[1], self)
                return True

        # Only varset[0], is now undefined.
        self.problem.watch(v, self)
        return self.problem.enqueue(self.varset[0], self)

    def undo(self, v):
        pass

    # Why is v True?
    # Or, why are we causing a conflict (if v is None)?
    def calc_reason(self, v):
        assert v is None or v == self.varset[0]

        # The cause is everything except v.
        return [-v_ for v_ in self.varset if v_ != v]

    def __repr__(self):
        return "<some: %r>" % self.varset


class _Problem(object):

    def __init__(self, max_var):
        # True/False/None
        self._vars = [None]
        # constraints to check when _vars[i] becomes True
        self._pos_watch = [None]
        # constraints to check when _vars[i] becomes False
        self._neg_watch = [None]
        # Constraints to update if we become unbound for every _vars[i]
        self.undo = [None]
        # The decision level at which we got a value for _vars[i]
        self._levels = [None]
        # The constraint that implied _vars, if True or False
        self._reasons = [None]
        # order of assignments
        self._trail = []
        # decision levels (len(_trail) at each decision)
        self._trail_lim = []
        # propagation queue
        self._propQ = deque()

        count = max_var - len(self._vars) + 1
        self._vars.extend([None] * count)
        self._pos_watch.extend([[] for __ in range(count)])
        self._neg_watch.extend([[] for __ in range(count)])
        self.undo.extend([[] for __ in range(count)])
        self._levels.extend([-1] * count)
        self._reasons.extend([None] * count)

    def watch(self, v, cb):
        (self._neg_watch[-v] if v < 0 else self._pos_watch[v]).append(cb)

    def value(self, v):
        assert v
        if v > 0:
            return self._vars[v]
        else:
            value = self._vars[-v]
            if value is None:
                return None
            else:
                return not value

    def add_clause(self, varset):
        _logger.trace('Add clause %r', varset)

        if any(self.value(v) is True for v in varset):
            # Trivially true already.
            return True
        varset_ = set(varset)
        for v in varset:
            if -v in varset_:
                # X or not(X) is always True.
                return True
        # Remove duplicates and values known to be False
        varset = [v for v in varset_ if self.value(v) is not False]

        return self._add_clause(varset, learnt=False, reason="input fact")

    def at_most_one(self, varset):
        _logger.trace('At most one of %r', varset)

        # If we have zero or one literals then we're trivially true
        # and not really needed for the solve. However, Zero Install
        # monitors these objects to find out what was selected, so
        # keep even trivial ones around for that.
        #
        #if len(varset) < 2:
        #    return True    # Trivially true

        # Ensure no duplicates
        assert len(set(varset)) == len(varset), varset

        # Ignore any literals already known to be False.
        # If any are True then they're enqueued and we'll process them
        # soon.
        varset = [v for v in varset if self.value(v) is not False]

        clause = _AtMostOneClause(self, varset)
        for v in varset:
            self.watch(v, clause)

        return clause

    def run_solver(self, decide):
        """@rtype: bool"""

        while True:
            # Use logical deduction to simplify the clauses
            # and assign literals where there is only one possibility.
            conflicting_clause = self._propagate()
            if not conflicting_clause:
                _logger.trace('New state: %r', self._vars[1:])
                if all(i is not None for i in self._vars[1:]):
                    # Everything is assigned without conflicts
                    _logger.trace('SUCCESS!')
                    return True
                else:
                    # Pick a variable and try assigning it one way.
                    # If it leads to a conflict, we'll backtrack and
                    # try it the other way.
                    v = decide()
                    assert v is not None, "decide function returned None!"
                    assert self.value(v) is None
                    self._trail_lim.append(len(self._trail))
                    r = self.enqueue(v, reason="considering")
                    assert r is True
            else:
                if self._decision_level == 0:
                    _logger.trace('FAIL: conflict found at top level')
                    return False
                else:
                    # Figure out the root cause of this failure.
                    learnt, backtrack_level = self._analyse(conflicting_clause)

                    self._cancel_until(backtrack_level)

                    clause = self._add_clause(learnt, learnt=True,
                            reason=conflicting_clause)
                    if clause is not True:
                        # Everything except the first literal in learnt
                        # is known to be False, so the first must be True.
                        enforce(self.enqueue(learnt[0], clause) is True)

    # v is now True
    # Returns False if this immediately causes a conflict.
    def enqueue(self, v, reason):
        _logger.trace('Enqueue %r', v)

        old_value = self.value(v)
        if old_value is not None:
            if old_value is False:
                # Conflict
                return False
            else:
                # Already set (shouldn't happen)
                return True

        v_abs = abs(v)
        self._vars[v_abs] = v > 0
        self._levels[v_abs] = self._decision_level
        self._reasons[v_abs] = reason
        self._trail.append(v)
        self._propQ.append(v)

        return True

    @property
    def _decision_level(self):
        return len(self._trail_lim)

    # Pop most recent assignment from self._trail
    def _undo_one(self):
        v = self._trail[-1]

        _logger.trace('Pop %s', v)

        v_abs = abs(v)
        self._vars[v_abs] = None
        self._levels[v_abs] = -1
        self._reasons[v_abs] = None
        self._trail.pop()

        undo = self.undo[v_abs]
        while undo:
            undo.pop().undo(v)

    def _cancel_until(self, level):
        while self._decision_level > level:
            n_this_level = len(self._trail) - self._trail_lim[-1]
            _logger.trace('Cancel at level %d (%d assignments)',
                    self._decision_level, n_this_level)
            while n_this_level != 0:
                self._undo_one()
                n_this_level -= 1
            self._trail_lim.pop()

    # Process the propQ.
    # Returns None when done, or the clause that caused a conflict.
    def _propagate(self):
        while self._propQ:
            watches_ = []
            v = self._propQ.popleft()
            if v < 0:
                watches, self._neg_watch[-v] = self._neg_watch[-v], watches_
            else:
                watches, self._pos_watch[v] = self._pos_watch[v], watches_

            _logger.trace('%s -> True : watches: %r', v, watches)

            for i, clause in enumerate(watches):
                if clause.propagate(v):
                    continue
                # Conflict, re-add remaining watches
                watches_.extend(watches[i + 1:])
                # No point processing the rest of the queue as
                # we'll have to backtrack now.
                self._propQ.clear()
                return clause

        return None

    # Returns the new clause if one was added, True if none was added
    # because this clause is trivially True, or False if the clause is
    # False.
    def _add_clause(self, varset, learnt, reason):
        if len(varset) == 1:
            # A clause with only a single literal is represented
            # as an assignment rather than as a clause.
            return self.enqueue(varset[0], reason)

        clause = _UnionClause(self, varset)

        if learnt:
            # varset[0] is None because we just backtracked.
            # Start watching the next literal that we will
            # backtrack over.
            best_level = -1
            best_i = 1
            for i in range(1, len(varset)):
                level = self._levels[abs(varset[i])]
                if level > best_level:
                    best_level = level
                    best_i = i
            varset[1], varset[best_i] = varset[best_i], varset[1]

        # Watch the first two literals in the clause (both must be
        # undefined at this point).
        for v in varset[:2]:
            self.watch(-v, clause)

        return clause

    def _analyse(self, cause):
        # After trying some assignments, we've discovered a conflict.
        # e.g.
        # - we selected A then B then C
        # - from A, B, C we got X, Y
        # - we have a rule: not(A) or not(X) or not(Y)
        #
        # The simplest thing to do would be:
        # 1. add the rule "not(A) or not(B) or not(C)"
        # 2. unassign C
        #
        # Then we we'd deduce not(C) and we could try something else.
        # However, that would be inefficient. We want to learn a more
        # general rule that will help us with the rest of the problem.
        #
        # We take the clause that caused the conflict ("cause") and
        # ask it for its cause. In this case:
        #
        #  A and X and Y => conflict
        #
        # Since X and Y followed logically from A, B, C there's no
        # point learning this rule; we need to know to avoid A, B, C
        # *before* choosing C. We ask the two varset deduced at the
        # current level (X and Y) what caused them, and work backwards.
        # e.g.
        #
        #  X: A and C => X
        #  Y: C => Y
        #
        # Combining these, we get the cause of the conflict in terms of
        # things we knew before the current decision level:
        #
        #  A and X and Y => conflict
        #  A and (A and C) and (C) => conflict
        #  A and C => conflict
        #
        # We can then learn (record) the more general rule:
        #
        #  not(A) or not(C)
        #
        # Then, in future, whenever A is selected we can remove C and
        # everything that depends on it from consideration.

        # The general rule we're learning
        learnt = [None]
        # The deepest decision in learnt
        learnt_level = 0
        # The literal we want to expand now
        p = None
        # The varset involved in the conflict
        seen = [False] * len(self._vars)
        counter = 0

        while True:
            # cause is the reason why p is True (i.e. it enqueued it).
            # The first time, p is None, which requests the reason
            # why it is conflicting.
            if p is None:
                p_reason = cause.calc_reason(p)
                _logger.trace('%s failed because of %r', cause, p_reason)
            else:
                p_reason = cause.calc_reason(p)
                _logger.trace('%s => %s because of %r', cause, p, p_reason)

            # p_reason is in the form (A and B and ...)
            # p_reason => p

            # Check each of the varset in p_reason that we haven't
            # already considered:
            # - if the variable was assigned at the current level,
            #   mark it for expansion
            # - otherwise, add it to learnt

            for v in p_reason:
                v_abs = abs(v)
                if seen[v_abs]:
                    continue
                seen[v_abs] = True
                level = self._levels[v_abs]
                if level == self._decision_level:
                    # We deduced this var since the last decision.
                    # It must be in self._trail, so we'll get to it
                    # soon. Remember not to stop until we've processed it.
                    counter += 1
                elif level > 0:
                    # We won't expand v, just remember it.
                    # (we could expand it if it's not a decision, but
                    # apparently not doing so is useful)
                    learnt.append(-v)
                    learnt_level = max(learnt_level, level)

            # At this point, counter is the number of assigned
            # varset in self._trail at the current decision level that
            # we've seen. That is, the number left to process. Pop
            # the next one off self._trail (as well as any unrelated
            # varset before it; everything up to the previous
            # decision has to go anyway).

            # On the first time round the loop, we must find the
            # conflict depends on at least one assignment at the
            # current level. Otherwise, simply setting the decision
            # variable caused a clause to conflict, in which case
            # the clause should have asserted not(decision-variable)
            # before we ever made the decision.
            # On later times round the loop, counter was already >
            # 0 before we started iterating over p_reason.
            assert counter > 0

            while True:
                p = self._trail[-1]
                cause = self._reasons[abs(p)]
                self._undo_one()
                if seen[abs(p)]:
                    break
                _logger.trace('(irrelevant)')
            counter -= 1

            if counter <= 0:
                assert counter == 0
                # If counter = 0 then we still have one more
                # literal (p) at the current level that we
                # could expand. However, apparently it's best
                # to leave this unprocessed (says the minisat
                # paper).
                break

        # p is the literal we decided to stop processing on. It's either
        # a derived variable at the current level, or the decision that
        # led to this level. Since we're not going to expand it, add it
        # directly to the learnt clause.
        learnt[0] = -p

        return learnt, learnt_level
