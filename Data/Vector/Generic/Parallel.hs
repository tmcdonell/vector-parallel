{-# LANGUAGE CPP, BangPatterns, FlexibleContexts, ScopedTypeVariables #-}
-- |
-- Module      : Data.Vector.Generic.Parallel
-- Copyright   : [2011] Trevor L. McDonell
-- License     : BSD3
--
-- Maintainer  : Trevor L. McDonell <tmcdonell@cse.unsw.edu.au>
-- Stability   : experimental
-- Portability : non-portable (GHC Extensions)
--

module Data.Vector.Generic.Parallel (

  -- * Construction
  enumFromN, enumFromStepN,

  -- * Element-wise operations
  map, imap, zip, zipWith,

  -- * Reductions
  fold, foldMap,

  -- ** Specialised reductions
  all, any, and, or, sum, product, maximum, minimum,

  -- * Re-exported for convenience
  module Data.Vector.Generic

) where

import           Prelude                                hiding (
  map, zip, zipWith, all, any, and, or, sum, product, maximum, minimum )
import qualified Prelude                                as P
import           Text.PrettyPrint
import           Control.Monad.Par
import           GHC.Conc                               ( numCapabilities )
import           System.IO.Unsafe                       ( unsafePerformIO )
import           Data.Vector.Generic                    ( Vector )
import qualified Data.Vector                            as V
import qualified Data.Vector.Generic                    as G
import qualified Data.Vector.Generic.Mutable            as M

-- Construction
-- ------------

-- | Yield a vector of the given length containing the values @x@, @x+1@ etc.
--
{-# INLINE enumFromN #-}
enumFromN :: (Vector v a, Num a) => a -> Int -> v a
enumFromN x n = enumFromStepN x 1 n

-- | Yield a vector of the given values containing the values @x@, @x+y@,
-- @x+y+y@, etc.
--
{-# INLINE enumFromStepN #-}
enumFromStepN :: forall v a. (Vector v a, Num a, M.MVector (G.Mutable v) a) => a -> a -> Int -> v a
enumFromStepN x y n =
  let dummy = G.create (M.unsafeNew n) :: v a
  in  imap (\i _ -> x + (fromIntegral i * y)) dummy


-- Mapping
-- -------

-- | Map a function to each element of an array, in parallel.
--
{-# INLINE map #-}
map :: (Vector v a, Vector v b) => (a -> b) -> v a -> v b
map f = unsplit . imapD (\_ x -> f x) . split

-- | Apply a function to every element of a vector and its index.
--
{-# INLINE imap #-}
imap :: (Vector v a, Vector v b) => (Int -> a -> b) -> v a -> v b
imap f = unsplit . imapD f . split


-- Zipping
-- -------

-- | Zip two vectors
--
{-# INLINE zip #-}
zip :: (Vector v a, Vector v b, Vector v (a,b)) => v a -> v b -> v (a,b)
zip = zipWith (,)

-- | Zip two vectors with the given function.
--
{-# INLINE zipWith #-}
zipWith :: (Vector v a, Vector v b, Vector v (a,b), Vector v c)
        => (a -> b -> c) -> v a -> v b -> v c
zipWith f x y = map (uncurry f) (G.zip x y)


-- Reductions
-- ----------

-- | Reduce an array to a single value. The combination function must be an
-- associative operation, and the stating element must be neutral with respect
-- to this operator; i.e. the pair must form a monoid. For example, @0@ is
-- neutral with respect to @(+)@, as @0 + a = a@.
--
-- These restrictions are required to support efficient parallel evaluation, as
-- the starting value may be used many times depending on the number of threads.
--
{-# INLINE fold #-}
fold :: Vector v a => (a -> a -> a) -> a -> v a -> a
fold c z = join c . ifoldD (\_ x y -> c x y) z . split

-- | A combination of 'map' followed by 'fold'. The same restrictions apply to
-- the reduction operator and neutral element.
--
{-# INLINE foldMap #-}
foldMap :: Vector v a => (a -> b) -> (b -> b -> b) -> b -> v a -> b
foldMap f c z = join c . ifoldD f' z . split
  where f' _ k x = k `c` f x


-- Specialised reductions
-- ----------------------

-- | Check if all elements satisfy the predicate
--
{-# INLINE all #-}
all :: Vector v a => (a -> Bool) -> v a -> Bool
all p = foldMap p (&&) True

-- | Check if any element satisfies the predicate
--
{-# INLINE any #-}
any :: Vector v a => (a -> Bool) -> v a -> Bool
any p = foldMap p (||) False

-- | Check if all elements are True
--
{-# INLINE and #-}
and :: Vector v Bool => v Bool -> Bool
and = fold (&&) True

-- | Check if any element is True
--
{-# INLINE or #-}
or :: Vector v Bool => v Bool -> Bool
or = fold (||) False

-- | Compute the sum of the elements
--
{-# INLINE sum #-}
sum :: (Vector v a, Num a) => v a -> a
sum = fold (+) 0

-- | Compute the product of the elements
--
{-# INLINE product #-}
product :: (Vector v a, Num a) => v a -> a
product = fold (*) 1

-- | Yield the maximum element of a non-empty vector
--
{-# INLINE maximum #-}
maximum :: (Vector v a, Ord a) => v a -> a
maximum v | G.null v    = error "maximum: empty vector"
          | otherwise   = fold max (G.unsafeHead v) (G.unsafeTail v)

-- | Yield the minimum element of a non-empty vector
--
{-# INLINE minimum #-}
minimum :: (Vector v a, Ord a) => v a -> a
minimum v | G.null v    = error "minimum: empty vector"
          | otherwise   = fold min (G.unsafeHead v) (G.unsafeTail v)


--
-- Distributed types -----------------------------------------------------------
--
-- [NOTE: Distributed types and unsafePerformIO]
--
-- We use unsafe functions in the core implementations so that threads can
-- destructively update the result vector in-place. This is particularly
-- important for 'map', where we would otherwise have to concatenate the result
-- vectors from each individual thread into the final contiguous vector,
-- incurring an additional O(n) overhead.
--
-- The unsafePerformIO on the worker function passed to each thread is less
-- concerning: the monad-par library uses 'deepseq' to ensure the computation of
-- each thread is fully evaluated. In this case, 'deepseq' is called on the unit
-- type, ensuring all preceding write operations to the mutable array have
-- completed, but happily also means we do not require NFData constraints on the
-- actual array type.
--
-- TLM: Maybe it is possible to do this under ST, rather than using unsafe
--      functions in IO?
--

-- Our distributed types consist of a [single] result vector (since we actually
-- only work with shared memory machines) and a description of how this should
-- be partitioned between the available threads.
--
-- The list of segments holds zero-indexed counters for, respectively: this
-- segment ID, offset of the first element, number of elements in this segment.
--
type Segments = [(Int,Int,Int)]
data Dist a   = Dist Segments a

-- Completely superfluous use of pretty-printing library for an internal data
-- structure.
--
instance Show a => Show (Dist a) where
  show (Dist s v)
    = render $ text "Dist" <+> braces
        (sep $ punctuate comma [text "segments" <+> equals <+> text (show s)
                               ,text  "payload" <+> equals <+> text (show v)
                               ])

-- There are four simplifier phases that count down to zero. We add phase
-- numbers to the inline pragmas so that certain functions are retained for
-- longer so that the fusion rules have a chance to fire.
--
#define FUSE  [1]
#define INNER [0]

#define INLINE_FUSE  INLINE FUSE
#define INLINE_INNER INLINE INNER

-- Map a function to each element of a distributed array, assigning one thread
-- per array segment.
--
{-# INLINE_FUSE imapD #-}
imapD :: (Vector v a, Vector v b) => (Int -> a -> b) -> Dist (v a) -> Dist (v b)
imapD f (Dist s v) = unsafePerformIO $ do
  mv <- M.unsafeNew (G.length v)
  runPar (parMap (fill mv) s) `seq` Dist s `fmap` G.unsafeFreeze mv
  where
    fill mv (_,start,n) = unsafePerformIO $ go start
      where
        !end              = start + n
        go !i | i >= end  = return ()
              | otherwise = M.unsafeWrite mv i (f i $ G.unsafeIndex v i) >> go (i+1)


-- Reduce each segment of a distributed array to a single value, given a
-- combination function and neutral element. The function is not associative so
-- that we can support fold/map fusion.
--
-- Additionally, the output vector type changes to a basic boxed vector so that
-- we don't require any class restrictions on the output type. The number of
-- segments is expected to be small, so this should not be too detrimental, but
-- we may want to revisit this once the API is expanded and there are later
-- fusion stages.
--
{-# INLINE_FUSE ifoldD #-}
ifoldD :: Vector v b => (Int -> a -> b -> a) -> a -> Dist (v b) -> Dist (V.Vector a)
ifoldD c z (Dist s v) = unsafePerformIO $ do
  mv <- M.unsafeNew (length s)
  v' <- runPar (parMap (reduce mv) s) `seq` G.unsafeFreeze mv
  return $ split v'
  where
    reduce mv (ix,start,n) = unsafePerformIO $ go start z
      where
        !end                 = start + n
        go !i !a | i >= end  = M.unsafeWrite mv ix a
                 | otherwise = go (i+1) (c i a $ G.unsafeIndex v i)


{-# RULES
"split/unsplit" forall v.       split (unsplit v)       = v
"imapD/imapD"   forall f g v.   imapD g (imapD f v)     = imapD (\i -> g i . f i) v
"imapD/ifoldD"  forall f c z v. ifoldD c z (imapD f v)  = ifoldD (\i k x -> c i k (f i x)) z v
  #-}


-- Extract the payload of a distributed type structure, discarding any
-- segmenting information. Note that this function can not be declared as part
-- of the datatype definition, because then the rewrite rules will not fire.
--
{-# INLINE_FUSE unsplit #-}
unsplit :: Dist a -> a
unsplit (Dist _ p) = p


-- Join distributed segments with the associative binary operator.
--
-- TODO: parallel binary-tree reduction.
--
{-# INLINE_FUSE join #-}
join :: Vector v a => (a -> a -> a) -> Dist (v a) -> a
join c (Dist _ r) = G.foldl1' c r


-- Split a vector into a distributed representation, where segments are sized as
-- evenly as possible. No actual data movement takes place.
--
{-# INLINE_FUSE split #-}
split :: Vector v a => v a -> Dist (v a)
split vec = Dist (len `into` pieces) vec
  where
    len    = G.length vec
    pieces = auto_partition_factor * numCapabilities


into :: Int -> Int -> Segments
into len pieces = P.map largepiece [0      .. remain-1] ++
                  P.map smallpiece [remain .. pieces-1]
  where
   (step, remain) = len `quotRem` pieces
   largepiece i   = (i, i * (step+1),      step+1)
   smallpiece i   = (i, i * step + remain, step)


-- How many tasks per process should we aim for.  Higher numbers improve load
-- balance but put more pressure on the scheduler.
--
auto_partition_factor :: Int
auto_partition_factor = 4


--
-- Miscellaneous ---------------------------------------------------------------
--

#if 0
putTraceMsg :: String -> IO ()
putTraceMsg msg = do
  tid <- myThreadId
  T.putTraceMsg (shows tid ": " ++ msg)


trace :: String -> a -> a
trace s a =
  let tid = unsafePerformIO myThreadId
      {-# NOINLINE tid #-}
  in  T.trace (shows tid ": " ++ s) a
#endif

