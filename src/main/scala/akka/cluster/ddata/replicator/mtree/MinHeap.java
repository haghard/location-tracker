package akka.cluster.ddata.replicator.mtree;

import java.util.*;

//https://github.com/mlarocca/AlgorithmsAndDataStructuresInAction

/**
 * Implementation of the PriorityQueue interface as a d-ary heap.
 * The branching factor for the heap can be passed as an argument.
 * It's 2 by default, but can be set up to 10.
 * The branching factor is the maximum number of children that each internal node can have.
 * For regular heaps, a node have at most 2 children, so the branching factor is 2.
 * The higher the branching factor, the shortest the height of the heap. However, when an element is
 * pushed towards the leaves of the heap, at each step all children of current node must be examined,
 * so a larger branching factor implies a higher number of nodes to be checked for each step of this
 * operation.
 * On the other hand, inserting elements only examines at most h element, where h is the height of the heap,
 * so this operation is only made faster with larger branching factors.
 * In general values between 3 and 5 are a good compromise and produce good performance.
 * <p>
 * Duplicates are not allowed in this implementation.
 * <p>
 * Performance:
 * - Add, top both take time proportional to the height of the tree, and so O(log n)
 * - size uses List::size, and so it's cached and requires constant time, O(1);
 * - contains requires O(1) average time, because this implementation uses a Hash Table to support fast contains (and fast update).
 * - update, remove requires O(log n) average time, because they can leverage the "fast" contains implementation.
 *
 * @param <T> The generic type for elements that can be held in the heap.
 *            <p>
 *            <p>
 *            MinHeap ia a complete binary tree where each parent has lesser value than its descendants.
 *            Root will have the smallest value(Elements arranged in thr asc order)
 *            <p>
 *            <p>
 *            Addition: Add at the end 0(1) and bubble up (max (O(logN))) Complexity: O(1) to O(logN). O(1) when we insert the highest element
 *            Removal:  Put last element at position of removed element(0(1)) and push down (max (O(logN))) Complexity: O(1) to O(logN). O(1) when we remove the highest element
 *            <p>
 *            Useful to implement LFUCache.
 *            <p>
 *            https://youtu.be/HqPJF2L5h9U
 *            <p>
 *            Optimal Binary Search Tree Successful and Unsuccessful Probability - Dynamic Programming https://www.youtube.com/watch?v=wAy6nDMPYAE
 */
final public class MinHeap<T extends Comparable<T>> {

    private static final int DEFAULT_BRANCHING_FACTOR = 2;

    public static final int MAX_BRANCHING_FACTOR = 10;

    private List<T> elements;

    /**
     * Keep the positions of the elements in a hash map to implement a fast version of `contains` method.
     */
    private Map<T, Integer> elementsPositions;

    /**
     * Branching factor for the heap, i.e. the (max) number of children for each node.
     */
    private int branchingFactor;

    /**
     * Default constructor. Branching factor is 2 by default, and the heap is initialized, but empty.
     */
    public MinHeap() {
        this(DEFAULT_BRANCHING_FACTOR);
    }

    /**
     * Constructor: takes the branching factor as a input.
     * Valid branching factors are integers between 2 and 10 included.
     * The heap is initialized and ready to hold elements.
     *
     * @param bFactor The (maximum) number of children that a node can have.
     * @throws IllegalArgumentException: - If the branching factor is not within the valid range.
     */
    public MinHeap(int bFactor) throws IllegalArgumentException {
        validateBranchingFactor(bFactor);
        elements = new ArrayList<>();
        //elementsPositions = HashMultimap.create();
        elementsPositions = new HashMap<>();
        branchingFactor = bFactor;
    }

    /**
     * Most generic constructor, using heapify to construct a heap from a list of elements.
     *
     * @param elems   A list of elements to add to the heap.
     * @param bFactor The (maximum) number of children that a node can have.
     * @throws IllegalArgumentException: If the branching factor is not within the valid range.
     */
    public MinHeap(List<T> elems, int bFactor) {
        if (elems == null) {
            new MinHeap(bFactor);
        } else {
            validateBranchingFactor(bFactor);
            branchingFactor = bFactor;
            elements = new ArrayList<>(elems);
            elementsPositions = new HashMap<>();

            //elementsPositions = HashMultimap.create();
            //Multimaps.newSetMultimap()
            //ArrayListMultimap.create()
            //HashMultimap.<Integer, Integer>create();
            //LinkedHashMultimap.create<T, Integer>();
            //TreeMultimap.create<T, Integer>();

            int n = elems.size();
            // Now performs a heapify initialization
            for (int i = getParentIndex(n - 1) + 1; i < n; i++) {
                // Sets the positions for the second half of the array
                elementsPositions.put(elements.get(i), i);
            }

            for (int i = getParentIndex(n - 1); i >= 0; i--) {
                // Performs a push-down for every element with at least one children, starting from last
                // This way each sub-tree rooted at index i will be a valid sub-heap
                pushDown(i);
            }
        }
    }

    /**
     * Removes the top element from the heap if it's not empty.
     * <p>
     *
     * @return If no element is present, returns an empty Optional. Otherwise, the smallest element.
     */
    public Optional<T> top() {
        if (isEmpty()) {
            return Optional.empty();
        }
        int n = elements.size();
        T top = elements.get(0);

        if (n > 1) {
            // Replaces the top element with the last element in the heap
            elements.set(0, elements.remove(n - 1));
            pushDown(0);
        } else {
            elements.remove(0);
        }
        elementsPositions.remove(top);
        // INVARIANT: top is non null at this point
        return Optional.of(top);
    }

    /**
     * Returns the top element of the heap (if it's not empty), without any side effect.
     * <p>
     * Thread safe.
     *
     * @return If no element is present, returns an empty Optional. Otherwise wraps the highest
     * priority element in an Optional container.
     */
    public Optional<T> peek() {
        return elements.isEmpty() ? Optional.empty() : Optional.of(elements.get(0));
    }

    /**
     * Check if an element is stored in the heap.
     * O(1) average time, because this implementation uses a Hash Table to support fast contains (and fast update).
     *
     * @param element The element of interest.
     *                <p>
     *                Thread safe.
     * @return true iff the element is present.
     */
    public boolean contains(T element) {
        return elementsPositions.containsKey(element);
    }

    /**
     * Add a new element to the heap. The heap does not allow duplicates, so if an element equals
     * to the argument is already stored in the heap, ignores the new one.
     * Takes time proportional to the height of the tree - O(log N).
     *
     * @param element The value of the element to add.
     *                <p>
     *                Thread safe.
     * @return true iff the element has been successfully added, false otherwise.
     */
    public boolean add(final T element) {
        if (contains(element)) {
            return false;
        } // else {

        elements.add(element);
        bubbleUp(elements.size() - 1);
        return true;

    }

    /**
     * Removes a generic element from the heap. It doesn't need to be the top element, as the removal
     * is based on its value, not on the priority.
     * Remove requires O(log n) average time, because they can leverage the "fast" contains implementation.
     *
     * @param element The element to be removed.
     *                <p>
     *                Thread safe.
     * @return true iff the element was stored in the heap and then correctly removed.
     */
    public Optional<T> remove(T element) {
        if (isEmpty() || !contains(element)) {
            return Optional.empty();
        } //else

        T removed;
        int n = size();
        int position = elementsPositions.get(element);
        if (position == n - 1) {
            // This also covers the case n == 1
            removed = elements.remove(position);
            elementsPositions.remove(element);
        } else {
            removed = elements.get(position);
            elements.set(position, elements.get(n - 1));
            elements.remove(n - 1);
            elementsPositions.remove(element);
            pushDown(position);
        }
        return Optional.of(removed);
    }

    /**
     * Updates an element already stored in the queue. This method's implementation is more efficient than removing
     * the old element and then adding the new one with two separate calls.
     *
     * @param oldElement The element to be updated. If the element is not in the queue, it will NOT be added.
     * @param newElement The new value for the element.
     *                   <p>
     * @return true if the element was stored in the heap and its priority successfully updated.
     */
    public boolean updatePriority(T oldElement, T newElement) {
        if (isEmpty() || !contains(oldElement)) {
            return false;
        } //else

        int position = elementsPositions.remove(oldElement);
        elements.set(position, newElement);

        if (hasHigherPriority(newElement, oldElement)) bubbleUp(position);
        else pushDown(position);

        return true;
    }

    /**
     * Returns the size of the heap. O(1)
     * <p>
     * Thread safe.
     *
     * @return The number of elements sorted in the heap.
     */
    public int size() {
        return elements.size();
    }

    /**
     * Remove all elements from the heap.
     * <p>
     * Thread safe.
     */

    public void clear() {
        elements.clear();
        elementsPositions.clear();
    }

    /**
     * Compares two elements stored in the heap and checks if the first one has higher priority than the second one.
     *
     * @param element              The element whose priority is checked.
     * @param withRespectToElement The second element, the one with respect to which the comparison is done.
     * @return true iff the first argument has higher priority than the second, in this heap.
     */
    protected boolean hasHigherPriority(T element, T withRespectToElement) {
        return element.compareTo(withRespectToElement) < 0;
    }

    /**
     * Computes the position of the first child of an element in a given position.
     * It might overflow the array, checks should be performed by the caller.
     *
     * @param index The position of the element whose children's position need to be computed.
     * @return The index that the first child of element at position `index` would have in the array.
     */
    protected int getFirstChildIndex(int index) {
        return branchingFactor * index + 1;
    }

    /**
     * Computes the position that the parent of current element
     * When called for the root, it might underflow the array (in theory, only with branching factor 1), or
     * return the same position: checks should be performed by the caller.
     *
     * @param index The index of the current element, whose parent's position needs to be computed.
     * @return
     */
    protected int getParentIndex(int index) {
        return (index - 1) / branchingFactor;
    }

    /**
     * Validates the value provided for the branching factor.
     * Valid range is between 2 and MAX_BRANCHING_FACTOR.
     *
     * @param branchingFactor The value to validate.
     * @throws IllegalArgumentException In case the argument is outside valid range.
     */
    private void validateBranchingFactor(int branchingFactor) throws IllegalArgumentException {
        if (branchingFactor < DEFAULT_BRANCHING_FACTOR || branchingFactor > MAX_BRANCHING_FACTOR) {
            throw new IllegalArgumentException(
                    String.format("Branching factor needs to be an int between {} and {}", DEFAULT_BRANCHING_FACTOR, MAX_BRANCHING_FACTOR));
        }
    }

    /**
     * Pushes down the element at the given position, towards the heap's leaves, in order to reinstate heap properties.
     * It also needs to update the hashmap holding the element positions.
     *
     * @param index The position in the heap where to start.
     */
    private void pushDown(int index) {
        // INVARIANT: index < n
        int n = elements.size();
        int smallestChildrenIndex = getFirstChildIndex(index);
        T element = elements.get(index);

        while (smallestChildrenIndex < n) {
            int lastChildrenIndexGuard = Math.min(getFirstChildIndex(index) + branchingFactor, n);
            // Find all
            for (int childrenIndex = smallestChildrenIndex; childrenIndex < lastChildrenIndexGuard; childrenIndex++) {
                if (hasHigherPriority(elements.get(childrenIndex), elements.get(smallestChildrenIndex))) {

                    smallestChildrenIndex = childrenIndex;
                }
            }
            T child = elements.get(smallestChildrenIndex);

            if (hasHigherPriority(child, element)) {
                elements.set(index, child);
                System.out.println(String.format("pushDown: %s %s", index, child));

                elementsPositions.put(child, index);
                index = smallestChildrenIndex;
                smallestChildrenIndex = getFirstChildIndex(index);
            } else {
                // The element is already in the right place
                break;
            }
        }

        elements.set(index, element);
        System.out.println(String.format("pushDown: %s %s", index, element));
        elementsPositions.put(element, index);
    }

    /**
     * Bubbles up the element at the given position, towards the root, in order to reinstate heap properties.
     * It also needs to update the hashmap holding the element positions.
     *
     * @param index The position in the heap where to start.
     */
    private void bubbleUp(int index) {
        // INVARIANT: 0 <= index < n
        int parentIndex;
        T element = elements.get(index);

        while (index > 0) {
            parentIndex = getParentIndex(index);
            T parent = elements.get(parentIndex);
            if (hasHigherPriority(element, parent)) {
                elements.set(index, parent);
                elementsPositions.put(parent, index);
                System.out.println(String.format("bubbleUp: set %s at %s", parent, index));
                index = parentIndex;
            } else {
                // The element is already in the right position
                break;
            }
        }
        elements.set(index, element);
        System.out.println(String.format("bubbleUp: set %s at %s", element, index));
        elementsPositions.put(element, index);
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    /*public Iterator<T> heapSort() {

        return new Iterator<T> () {
            int counter = 0;
            @Override
            public boolean hasNext() {
                counter++;
                return counter <= size();
                //!isEmpty();
            }

            @Override
            public T next() {
                return peek().get();
                //return top().get();
            }
        };
    }*/

    public Iterator<T> heapSort() {

        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                return !isEmpty();
            }

            @Override
            public T next() {
                return top().get();
            }
        };
    }

    public boolean checkHeapInvariants() {
        for (int i = 0, n = size(); i < n; i++) {
            T parent = elements.get(i);

            for (int j = getFirstChildIndex(i), last = getFirstChildIndex(i + 1); j < last; j++) {
                if (j < n && hasHigherPriority(elements.get(j), parent)) {
                    return false;
                }
            }
        }
        return true;
    }

    public String positions() {
        return elementsPositions.toString();
    }

    @Override
    public String toString() {
        return elements.toString();
    }
}

/*
import com.codelfsolutions.ds.Item
import com.codelfsolutions.ds.MinHeap
val h = new MinHeap[Item](2)
h.add(Item("a"))
h.add(Item("b"))
h.add(Item("c"))
h.updatePriority(Item("a"), Item("a", 3))
h.updatePriority(Item("b"), Item("b", 2))
h.updatePriority(Item("c"), Item("c", 1))
*/